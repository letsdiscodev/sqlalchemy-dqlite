"""Pin: a ``CancelledError`` from ``cur.close()`` during cleanup must not
replace the original execute exception SA's classifier expects to see.
"""

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection


async def test_execute_suppresses_cancellederror_from_cursor_close() -> None:
    """If ``cur.close()`` raises ``CancelledError`` during cleanup,
    the original ``execute`` exception must propagate, not the cancel."""

    class _ExecBoom(Exception):
        pass

    cursor_close = MagicMock(side_effect=asyncio.CancelledError())
    cursor_execute = MagicMock(side_effect=_ExecBoom("primary"))

    cur = MagicMock()
    cur.close = cursor_close
    cur.execute = cursor_execute

    adapter: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    with (
        patch.object(AsyncAdaptedConnection, "cursor", return_value=cur),
        pytest.raises(_ExecBoom),
    ):
        await greenlet_spawn(adapter.execute, "SELECT 1")
    cursor_close.assert_called_once()


async def test_execute_clean_path_returns_cursor() -> None:
    """Regression guard: the happy-path return value is unchanged."""
    cur = MagicMock()
    cur.execute = MagicMock()

    adapter: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    with patch.object(AsyncAdaptedConnection, "cursor", return_value=cur):
        result = await greenlet_spawn(adapter.execute, "SELECT 1")
    assert result is cur
    cur.execute.assert_called_once_with("SELECT 1")
