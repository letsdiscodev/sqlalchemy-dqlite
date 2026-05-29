"""The untracked-SAVEPOINT InterfaceError reports a programmer mistake on a
healthy connection and must NOT classify as a disconnect.

Guards against a future widening (e.g. adding "savepoint") that would mask
the diagnostic with pool slot churn.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect

# Verbatim copy of DqliteConnection.transaction()'s wording; a client
# wording change must update this string and re-verify classification.
_UNTRACKED_SAVEPOINT_MESSAGE = (
    "Cannot start transaction: a SAVEPOINT outside an explicit "
    "BEGIN is currently open on this connection (the SQLite "
    "engine has auto-begun a transaction). Issue COMMIT / "
    "ROLLBACK or RELEASE the outer SAVEPOINT before entering "
    "transaction()."
)


def test_untracked_savepoint_interfaceerror_not_classified_as_disconnect() -> None:
    dialect = DqliteDialect()
    exc = InterfaceError(_UNTRACKED_SAVEPOINT_MESSAGE)

    assert dialect.is_disconnect(exc, None, None) is False, (
        "untracked-SAVEPOINT InterfaceError reports a programmer "
        "mistake on a healthy connection — must NOT trigger pool "
        "invalidation. A regression here would mask the diagnostic "
        "with slot churn."
    )


def test_savepoint_substring_alone_does_not_classify_as_disconnect() -> None:
    """A bare InterfaceError mentioning "savepoint" must stay non-disconnect."""
    dialect = DqliteDialect()
    exc = InterfaceError("savepoint name parse error: bad identifier")

    assert dialect.is_disconnect(exc, None, None) is False


def test_connection_closed_interfaceerror_remains_classified_as_disconnect() -> None:
    """Positive control: "Connection is closed" must still classify."""
    dialect = DqliteDialect()
    exc = InterfaceError("Connection is closed")

    assert dialect.is_disconnect(exc, None, None) is True


def test_cursor_closed_interfaceerror_remains_classified_as_disconnect() -> None:
    """Positive control: "Cursor is closed" must still classify."""
    dialect = DqliteDialect()
    exc = InterfaceError("Cursor is closed")

    assert dialect.is_disconnect(exc, None, None) is True
