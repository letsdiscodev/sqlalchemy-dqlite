"""Negative pin: the InterfaceError raised by the client's
``transaction()`` context-manager guard for "untracked SAVEPOINT
outside an explicit BEGIN" must NOT be classified as a disconnect.

The client's diagnostic surfaces a programmer mistake — the user
opened a quoted/untracked SAVEPOINT (which the SQLite engine
auto-begins a transaction on) and then tried to enter the
``transaction()`` ctxmgr without first issuing COMMIT/ROLLBACK or
RELEASE. The connection itself is healthy. SA's ``is_disconnect``
must return False so the SA pool surfaces the diagnostic to the
caller instead of invalidating the slot and silently retrying.

Today's classifier matches InterfaceError ONLY on
``"connection is closed"`` / ``"cursor is closed"`` substrings —
this message contains neither, so it correctly returns False. The
risk this pin closes: a future widening of the InterfaceError
substring set (e.g., adding ``"savepoint"``) would silently start
classifying programmer-error diagnostics as disconnects, masking
them with slot churn instead of clear errors. A negative pin
prevents that.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect

# Verbatim copy of the wording emitted by
# ``DqliteConnection.transaction()`` when ``_has_untracked_savepoint``
# is set without an explicit BEGIN. A future wording change in the
# client requires updating this string AND re-verifying the
# classification — the coupling is intentional.
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
    """Adjacent negative pin: a bare InterfaceError mentioning
    ``"savepoint"`` must also stay non-disconnect. Catches a
    future widening that adds ``"savepoint"`` to the InterfaceError
    substring set on the (incorrect) intuition that any
    SAVEPOINT-adjacent error means the slot is broken."""
    dialect = DqliteDialect()
    exc = InterfaceError("savepoint name parse error: bad identifier")

    assert dialect.is_disconnect(exc, None, None) is False


def test_connection_closed_interfaceerror_remains_classified_as_disconnect() -> None:
    """Positive control: ``"Connection is closed"`` MUST still
    classify as a disconnect. Without this, a refactor that broke
    the InterfaceError substring branch entirely would not be
    caught by the negative pins above."""
    dialect = DqliteDialect()
    exc = InterfaceError("Connection is closed")

    assert dialect.is_disconnect(exc, None, None) is True


def test_cursor_closed_interfaceerror_remains_classified_as_disconnect() -> None:
    """Positive control: ``"Cursor is closed"`` MUST also still
    classify as a disconnect. Same rationale as the connection-
    closed control above."""
    dialect = DqliteDialect()
    exc = InterfaceError("Cursor is closed")

    assert dialect.is_disconnect(exc, None, None) is True
