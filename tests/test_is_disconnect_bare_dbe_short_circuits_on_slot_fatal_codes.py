"""Pin: the bare-DatabaseError arm short-circuits return True for slot-fatal codes
(SQLITE_CORRUPT/FORMAT/NOTADB) regardless of message, mirroring do_ping.

The disconnect substring list omits the canonical engine wordings for these codes, so
the old substring-gated path returned False and the pool kept reusing a corrupt slot —
a silent data-correctness hazard without pool_pre_ping=True."""

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
        # Control: messages matching no disconnect substring.
        (SQLITE_CORRUPT, "irrecoverable on-disk page"),
        (SQLITE_NOTADB, "header check failed"),
    ],
)
def test_bare_database_error_with_slot_fatal_code_classifies_as_disconnect(
    code: int, message: str
) -> None:
    exc = DatabaseError(message, code=code)
    assert DqliteDialect().is_disconnect(exc, None, None) is True


def test_bare_database_error_with_non_slot_fatal_code_does_not_classify() -> None:
    """A non-slot-fatal code must not classify, even with a transport-shaped message."""
    exc = DatabaseError("wire decode failed near column 42", code=99)
    assert DqliteDialect().is_disconnect(exc, None, None) is False


def test_subclasses_of_database_error_are_not_routed_through_bare_arm() -> None:
    """Subclasses must not match the slot-fatal short-circuit (type-identity), else SA's
    retry would duplicate non-idempotent INSERTs."""
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
    """Parity: is_disconnect and do_ping must agree on the slot-fatal code set (the SSOT
    reason for extracting _BARE_DBE_DISCONNECT_CODES)."""
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
