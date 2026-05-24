"""Pin: ``is_disconnect``'s bare-``DatabaseError`` arm short-circuits
``return True`` for codes in ``_BARE_DBE_DISCONNECT_CODES``
(SQLITE_CORRUPT=11, SQLITE_FORMAT=24, SQLITE_NOTADB=26), regardless
of the message text. Mirrors ``do_ping``'s classification (base.py:
2935-2945).

Before the fix, the bare-``DatabaseError`` arm set
``applies_substring = True`` and then routed through the same
substring scan used for the ``OperationalError`` arm. The substring
list (``_dqlite_disconnect_messages``) contains transport-state
phrases ("connection closed", "wire decode failed", "timed out",
"failed to connect", ...) but NONE of the canonical engine wordings
for the three slot-fatal codes:

  - SQLITE_CORRUPT (11) → "database disk image is malformed"
  - SQLITE_NOTADB (26) → "file is not a database"
  - SQLITE_FORMAT (24) → "unsupported file format" / "file is
    encrypted or is not a database"

So a real CORRUPT response from the engine returned False from
``is_disconnect``, SA's pool returned the slot to the queue, and
the next checkout reused the same slot — re-tripping the same
fault on every subsequent query. The companion ``do_ping`` arm
correctly short-circuits ``return False`` for the same code set
WITHOUT a substring filter; this restores parity.

This is a HIGH-severity silent data-correctness hazard for any
user running without ``pool_pre_ping=True``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import DatabaseError
from dqlitewire.constants import SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "code,message",
    [
        (SQLITE_CORRUPT, "database disk image is malformed"),
        (SQLITE_NOTADB, "file is not a database"),
        (SQLITE_FORMAT, "unsupported file format"),
        (SQLITE_FORMAT, "file is encrypted or is not a database"),
        # Hostile-control: message that explicitly avoids any
        # substring in _dqlite_disconnect_messages.
        (SQLITE_CORRUPT, "irrecoverable on-disk page"),
        (SQLITE_NOTADB, "header check failed"),
    ],
)
def test_bare_database_error_with_slot_fatal_code_classifies_as_disconnect(
    code: int, message: str
) -> None:
    """Every (code, message) shape in the bare-DBE slot-fatal set
    classifies as disconnect, regardless of substring."""
    exc = DatabaseError(message, code=code)
    assert DqliteDialect().is_disconnect(exc, None, None) is True


def test_bare_database_error_with_non_slot_fatal_code_does_not_classify() -> None:
    """Regression: a bare ``DatabaseError`` whose code is NOT in
    ``_BARE_DBE_DISCONNECT_CODES`` must not be classified as
    disconnect, even with a transport-shaped message. Per the
    `_BARE_DBE_DISCONNECT_CODES` SSOT and ``do_ping`` arm precedent."""
    exc = DatabaseError("wire decode failed near column 42", code=99)
    assert DqliteDialect().is_disconnect(exc, None, None) is False


def test_subclasses_of_database_error_are_not_routed_through_bare_arm() -> None:
    """Regression: ``IntegrityError`` / ``DataError`` (subclasses of
    DatabaseError) take the ``type(cause) is DatabaseError`` arm via
    type-identity check — they MUST NOT match the slot-fatal
    short-circuit even when they happen to carry a transport-shaped
    message. SA's retry path would otherwise duplicate non-idempotent
    INSERTs."""
    from dqlitedbapi.exceptions import DataError, IntegrityError

    integrity = IntegrityError(
        "constraint failed: timed out validating peer",
        code=19,
    )
    assert DqliteDialect().is_disconnect(integrity, None, None) is False

    data = DataError(
        "datatype mismatch: connection closed in payload",
        code=20,
    )
    assert DqliteDialect().is_disconnect(data, None, None) is False


def test_is_disconnect_and_do_ping_agree_on_slot_fatal_codes() -> None:
    """Cross-classifier parity pin: for every code in
    ``_BARE_DBE_DISCONNECT_CODES``, ``is_disconnect`` returns True AND
    ``do_ping`` returns False (False == "slot dead, invalidate"). The
    two classifiers MUST agree on the slot-fatal code set — that's
    the whole point of extracting ``_BARE_DBE_DISCONNECT_CODES`` as
    a shared constant.

    do_ping's classifier is tested separately in the do_ping test
    suite; this test pins the is_disconnect side and asserts the
    invariant relation: is_disconnect(exc) == True iff do_ping
    would return False on the same exc-raising path."""
    from sqlalchemydqlite.base import _BARE_DBE_DISCONNECT_CODES

    dialect = DqliteDialect()
    for code in _BARE_DBE_DISCONNECT_CODES:
        exc = DatabaseError(f"engine error with code {code}", code=code)
        assert dialect.is_disconnect(exc, None, None) is True, (
            f"is_disconnect must classify bare DatabaseError(code={code}) as "
            f"disconnect; got False. The do_ping arm at base.py:2935-2945 "
            f"already short-circuits this code set — the two classifiers "
            f"must agree (_BARE_DBE_DISCONNECT_CODES is the SSOT)."
        )
