"""``AsyncAdaptedConnection.autocommit`` is a read-only property reporting
``False`` (the effective dqlite mode) and rejecting ``True`` with
``ArgumentError``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _adapter() -> AsyncAdaptedConnection:
    raw = MagicMock()
    return AsyncAdaptedConnection(raw)


def test_autocommit_reports_false() -> None:
    assert _adapter().autocommit is False


def test_autocommit_getattr_default_is_false_not_none() -> None:
    """SA's ``getattr(conn, 'autocommit', None)`` probe must see ``False``,
    not ``None`` (which would log "autocommit unknown")."""
    assert getattr(_adapter(), "autocommit", None) is False


def test_autocommit_setter_rejects_true() -> None:
    a = _adapter()
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        a.autocommit = True


def test_autocommit_setter_accepts_false_as_noop() -> None:
    a = _adapter()
    a.autocommit = False
    assert a.autocommit is False
