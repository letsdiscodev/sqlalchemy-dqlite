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
