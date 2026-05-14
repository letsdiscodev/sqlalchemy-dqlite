"""Pin: ``is_disconnect`` substring fallback fires for bare ``DatabaseError``.

``_classify_operational`` routes SQLite codes 11/24/26
(CORRUPT/FORMAT/NOTADB) to bare ``DatabaseError`` (not
``OperationalError``). The substring fallback must compare the
disconnect-message patterns against ``DatabaseError`` causes too —
otherwise a "wire stream error" / "wire decode failed" tail glued to a
CORRUPT-coded server message would leak past the classifier and the
pool would keep a desynced wire socket.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import DatabaseError
from sqlalchemydqlite.base import DqliteDialect


def test_database_error_with_wire_decode_substring_classifies_as_disconnect() -> None:
    """Bare ``DatabaseError`` (CORRUPT, code=11) carrying a
    transport-shaped substring must be classified as a disconnect.
    Without the widening this fell straight through to
    ``super().is_disconnect`` → False, leaving the slot in the pool."""
    e = DatabaseError(
        "database disk image is malformed; wire decode failed at offset 42",
        code=11,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_database_error_without_disconnect_substring_not_classified() -> None:
    """Negative pin: a bare ``DatabaseError`` whose message does NOT
    contain a disconnect substring must NOT be classified as a
    disconnect — the widening only wires the substring branch to the
    larger class hierarchy; it does not turn every ``DatabaseError``
    into a disconnect."""
    e = DatabaseError("database disk image is malformed", code=11)
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_database_error_via_cause_walk() -> None:
    """The widening also benefits the cause-walk: a bare
    ``DatabaseError`` wrapped inside an unrelated exception must still
    classify via the substring fallback."""
    inner = DatabaseError(
        "database disk image is malformed; connection closed",
        code=11,
    )

    class _MyAppError(Exception):
        pass

    outer = _MyAppError("application wrapper")
    outer.__cause__ = inner
    assert DqliteDialect().is_disconnect(outer, None, None) is True


def test_integrity_error_with_disconnect_substring_not_classified() -> None:
    """Negative pin for the substring widening false-positive surface:
    an ``IntegrityError`` whose message contains a transport-shaped
    substring (e.g., a user-defined ``RAISE(ABORT, '...timed out...')``
    in a constraint trigger) must NOT be classified as a disconnect.

    Without this guard, SA pool would invalidate the slot and retry —
    duplicating non-idempotent INSERTs.

    The substring scan must be restricted to OperationalError + bare
    DatabaseError with the BARE_DATABASE_ERROR_CODES set (11/24/26);
    IntegrityError (code 19), DataError, etc. must NOT match the
    substring branch.
    """
    from dqlitedbapi.exceptions import IntegrityError

    e = IntegrityError(
        "constraint failed: timed out validating peer",
        code=19,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_data_error_with_disconnect_substring_not_classified() -> None:
    """Symmetric to ``IntegrityError``: ``DataError`` (also a
    DatabaseError subclass) carrying a substring like ``"connection
    closed"`` in a server message must NOT classify as disconnect."""
    from dqlitedbapi.exceptions import DataError

    e = DataError(
        "datatype mismatch: connection closed in payload",
        code=20,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_bare_database_error_with_non_motivating_code_not_classified() -> None:
    """Pin the code-restriction explicitly: bare ``DatabaseError`` with
    a code OUTSIDE {11, 24, 26} (i.e. outside
    ``BARE_DATABASE_ERROR_CODES``) must not match the substring
    branch even with a transport-shaped message."""
    e = DatabaseError(
        "wire decode failed in column 42",
        code=99,  # not in {11, 24, 26}
    )
    assert DqliteDialect().is_disconnect(e, None, None) is False
