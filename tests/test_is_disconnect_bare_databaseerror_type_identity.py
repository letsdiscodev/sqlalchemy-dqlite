"""Pin: is_disconnect's bare-DatabaseError arm uses type identity, NOT isinstance.

isinstance would catch every DatabaseError subclass via MRO, so a caller-bug
ProgrammingError carrying a slot-fatal code could classify as disconnect and have SA's
retry duplicate non-idempotent INSERTs."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    ProgrammingError,
)
from sqlalchemydqlite.base import _BARE_DBE_DISCONNECT_CODES, DqliteDialect


def _dialect() -> DqliteDialect:
    return DqliteDialect.__new__(DqliteDialect)


_SUBCLASSES = [
    IntegrityError,
    InternalError,
    ProgrammingError,
    DataError,
    NotSupportedError,
]


@pytest.mark.parametrize("subclass", _SUBCLASSES)
@pytest.mark.parametrize(
    "code",
    [
        # Slot-fatal codes: would leak via isinstance; type-identity blocks them.
        *list(_BARE_DBE_DISCONNECT_CODES),
        21,  # SQLITE_MISUSE
        25,  # SQLITE_RANGE
        None,
    ],
)
@pytest.mark.parametrize(
    "message",
    [
        "wire decode failed validation",  # would match disconnect substring
        "constraint violation: NOT NULL",  # benign caller-bug message
    ],
)
def test_databaseerror_subclasses_do_not_classify_as_disconnect(
    subclass: type[DatabaseError], code: int | None, message: str
) -> None:
    if code is None:
        exc = subclass(message)
    else:
        try:
            exc = subclass(message, code=code)
        except TypeError:
            # NotSupportedError doesn't accept the code kwarg.
            pytest.skip(f"{subclass.__name__} does not accept code kwarg")
            return
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is False, (
        f"{subclass.__name__}(code={code}, message={message!r}) must "
        f"not classify as disconnect — caller-bug surfaces must "
        f"propagate to surface programmer errors."
    )


@pytest.mark.parametrize("code", list(_BARE_DBE_DISCONNECT_CODES))
def test_bare_databaseerror_with_disconnect_code_and_substring_does_classify(
    code: int,
) -> None:
    """Positive side: a bare DatabaseError with a slot-fatal code classifies as disconnect."""
    exc = DatabaseError("wire decode failed validation", code=code)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True


@pytest.mark.parametrize("code", list(_BARE_DBE_DISCONNECT_CODES))
def test_bare_databaseerror_with_disconnect_code_classifies_regardless_of_message(
    code: int,
) -> None:
    """A slot-fatal code classifies regardless of message — the code is the signal."""
    exc = DatabaseError("some unrelated server message", code=code)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True
