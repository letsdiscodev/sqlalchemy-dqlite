"""Pin: ``is_disconnect``'s bare-``DatabaseError`` arm uses type
identity (``type(cause) is DatabaseError``), NOT ``isinstance``.

The dbapi class hierarchy has many ``DatabaseError`` subclasses
(``OperationalError``, ``IntegrityError``, ``InternalError``,
``ProgrammingError``, ``DataError``, ``NotSupportedError``). An
``isinstance`` check would catch all of them via MRO and route them
through the bare-``DatabaseError`` substring scan; if any future
release ever assigns CORRUPT / FORMAT / NOTADB as an extended code on
a subclass, a caller-bug ``ProgrammingError`` could silently classify
as a disconnect — SA's retry path would then re-run the broken caller
code against a fresh connection, duplicating non-idempotent INSERTs.

Pin every ``DatabaseError`` subclass × benign-and-disconnect-code-and-
message permutation as ``is_disconnect() is False``.
"""

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


# Subclasses of DatabaseError that must NOT trigger disconnect
# classification via the bare-DatabaseError arm.
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
        # Codes inside _BARE_DBE_DISCONNECT_CODES: would activate the
        # substring scan via isinstance leakage; type-identity blocks
        # this.
        *list(_BARE_DBE_DISCONNECT_CODES),
        # Codes outside: would be inert today via the code-set
        # intersection, included for completeness.
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
    """Every ``DatabaseError`` subclass is a caller-bug surface or a
    syntactic-error surface and MUST propagate unchanged. The type-
    identity check on the bare-``DatabaseError`` arm enforces this."""
    if code is None:
        exc = subclass(message)
    else:
        try:
            exc = subclass(message, code=code)
        except TypeError:
            # NotSupportedError doesn't accept code kwarg; skip the
            # coded permutation for it.
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
    """Sanity: a BARE ``DatabaseError`` (not a subclass) with a code in
    ``_BARE_DBE_DISCONNECT_CODES`` and a disconnect-style message DOES
    classify as disconnect — pin the type-identity arm's positive
    side too."""
    exc = DatabaseError("wire decode failed validation", code=code)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True


@pytest.mark.parametrize("code", list(_BARE_DBE_DISCONNECT_CODES))
def test_bare_databaseerror_with_disconnect_code_no_substring_does_not_classify(
    code: int,
) -> None:
    """Sanity: bare ``DatabaseError`` with disconnect code but a
    non-matching message does NOT classify (substring is required)."""
    exc = DatabaseError("some unrelated server message", code=code)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is False
