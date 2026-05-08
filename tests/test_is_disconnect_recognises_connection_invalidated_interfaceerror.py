"""Pin: ``is_disconnect`` recognises the dbapi's
``InterfaceError("Connection invalidated (id=...); reconnect before
retrying commit / rollback. ...")`` raise with ``code=None`` so the
SA pool reclaims the slot after a sibling-task ``_invalidate``.

Threat model: leader-flip + retry pattern. Cancel-during-execute
lands on the client; ``_invalidate`` nulls ``_protocol``. The
caller catches the cancel and retries via SA pool checkout. The
dbapi pre-lock guard at ``aio/connection.py:1124-1130`` (and three
sibling sites) raises ``InterfaceError("Connection invalidated
(id=...); ...")`` with ``code=None``. Without recognition in
``is_disconnect``, the SA pool keeps the slot and the next checkout
returns the same dead inner connection.

The fix matches the lowercased substring ``"connection invalidated"``
on the InterfaceError arm of ``is_disconnect``, alongside the
existing ``"connection is closed"`` / ``"cursor is closed"`` cases.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect


def _make_dialect() -> DqliteDialect:
    """Construct a minimal DqliteDialect for is_disconnect testing."""
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
    """Regression guard: a user-raised InterfaceError that doesn't
    contain 'connection invalidated' / 'connection is closed' /
    'cursor is closed' must NOT be classified as disconnect."""
    dialect = _make_dialect()
    e = InterfaceError("user-raised: bad parameter shape")
    assert dialect.is_disconnect(e, None, None) is False


def test_existing_connection_is_closed_still_recognised() -> None:
    """Regression guard for the prior arm."""
    dialect = _make_dialect()
    e = InterfaceError("connection is closed (id=99)")
    assert dialect.is_disconnect(e, None, None) is True


def test_existing_cursor_is_closed_still_recognised() -> None:
    """Regression guard for the prior arm."""
    dialect = _make_dialect()
    e = InterfaceError("cursor is closed (id=99)")
    assert dialect.is_disconnect(e, None, None) is True


def test_connection_invalidated_match_is_case_insensitive() -> None:
    """Match against ``raw.lower()``; uppercase variant must also
    classify."""
    dialect = _make_dialect()
    e = InterfaceError("CONNECTION INVALIDATED somewhere")
    assert dialect.is_disconnect(e, None, None) is True
