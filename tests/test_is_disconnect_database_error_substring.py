"""is_disconnect substring fallback fires for bare DatabaseError.

Codes 11/24/26 (CORRUPT/FORMAT/NOTADB) route to bare DatabaseError, so the
substring fallback must scan those causes too or a desynced wire socket stays pooled.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import DatabaseError
from sqlalchemydqlite.base import DqliteDialect


def test_database_error_with_wire_decode_substring_classifies_as_disconnect() -> None:
    """Bare DatabaseError (CORRUPT, code=11) with a transport-shaped substring is a disconnect."""
    e = DatabaseError(
        "database disk image is malformed; wire decode failed at offset 42",
        code=11,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_database_error_with_slot_fatal_code_classifies_even_without_substring() -> None:
    """Bare DatabaseError with a slot-fatal code (11/24/26) classifies regardless of message."""
    e = DatabaseError("database disk image is malformed", code=11)
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_database_error_via_cause_walk() -> None:
    """A bare DatabaseError wrapped in an unrelated exception classifies via the cause-walk."""
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
    """IntegrityError with a transport-shaped substring must NOT classify, else SA
    retries and duplicates non-idempotent INSERTs. Substring scan is restricted to
    OperationalError + bare DatabaseError codes 11/24/26."""
    from dqlitedbapi.exceptions import IntegrityError

    e = IntegrityError(
        "constraint failed: timed out validating peer",
        code=19,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_data_error_with_disconnect_substring_not_classified() -> None:
    """DataError (a DatabaseError subclass) with a disconnect substring must NOT classify."""
    from dqlitedbapi.exceptions import DataError

    e = DataError(
        "datatype mismatch: connection closed in payload",
        code=20,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_bare_database_error_with_non_motivating_code_not_classified() -> None:
    """Bare DatabaseError with a code outside {11, 24, 26} must not match the substring branch."""
    e = DatabaseError(
        "wire decode failed in column 42",
        code=99,  # not in {11, 24, 26}
    )
    assert DqliteDialect().is_disconnect(e, None, None) is False
