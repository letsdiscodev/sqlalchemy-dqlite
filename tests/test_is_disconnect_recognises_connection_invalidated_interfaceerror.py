"""is_disconnect recognises the dbapi's "Connection invalidated (id=...)"
InterfaceError so the SA pool reclaims the slot.

The ``(id=`` parenthesis is the dbapi's contract at every raise site;
matching it avoids false positives on user-raised InterfaceErrors.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect


def _make_dialect() -> DqliteDialect:
    d = DqliteDialect()
    return d


def test_connection_invalidated_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "Connection invalidated (id=12345); reconnect before retrying "
        "commit / rollback. The inner transport was nulled by a sibling "
        "task's _invalidate."
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_unrelated_interfaceerror_not_classified_as_disconnect() -> None:
    """A user-raised InterfaceError with no disconnect substring → False."""
    dialect = _make_dialect()
    e = InterfaceError("user-raised: bad parameter shape")
    assert dialect.is_disconnect(e, None, None) is False


def test_unrelated_connection_invalidated_text_not_classified_as_disconnect() -> None:
    """Text with "connection invalidated" but no ``(id=`` form → False."""
    dialect = _make_dialect()
    e = InterfaceError("Connection invalidated by user trigger")
    assert dialect.is_disconnect(e, None, None) is False


def test_existing_connection_is_closed_still_recognised() -> None:
    dialect = _make_dialect()
    e = InterfaceError("connection is closed (id=99)")
    assert dialect.is_disconnect(e, None, None) is True


def test_existing_cursor_is_closed_still_recognised() -> None:
    dialect = _make_dialect()
    e = InterfaceError("cursor is closed (id=99)")
    assert dialect.is_disconnect(e, None, None) is True


def test_connection_invalidated_match_is_case_insensitive() -> None:
    """Match is case-insensitive; an uppercase ``(ID=N)`` variant classifies."""
    dialect = _make_dialect()
    e = InterfaceError("CONNECTION INVALIDATED (ID=42); reconnect")
    assert dialect.is_disconnect(e, None, None) is True
