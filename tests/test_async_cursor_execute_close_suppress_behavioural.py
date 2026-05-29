"""``execute``/``executemany`` cursor-close ``finally`` is leak-preventing AND
narrow-suppress: the close runs on body raise, a secondary close failure is
suppressed in favour of the primary, but BaseException (KI/SystemExit) is not."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_cursor_with_inner(
    inner_cursor: MagicMock,
) -> AsyncAdaptedCursor:
    adapter: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner_conn = MagicMock()
    inner_conn.cursor.return_value = inner_cursor
    adapter._connection = inner_conn
    return AsyncAdaptedCursor(adapter)


async def test_execute_closes_cursor_even_on_execute_raise() -> None:
    """Leak prevention: execute body raise -> underlying cursor.close runs."""
    inner_cursor = MagicMock()
    inner_cursor.execute = MagicMock(side_effect=OperationalError("body raise"))
    inner_cursor.close = MagicMock()

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(OperationalError, match="body raise"):
        await greenlet_spawn(cur.execute, "SELECT 1")

    inner_cursor.close.assert_called_once()


async def test_executemany_closes_cursor_even_on_executemany_raise() -> None:
    """Same contract, executemany surface."""
    inner_cursor = MagicMock()
    inner_cursor.executemany = MagicMock(side_effect=OperationalError("body raise emy"))
    inner_cursor.close = MagicMock()

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(OperationalError, match="body raise emy"):
        await greenlet_spawn(cur.executemany, "INSERT INTO t VALUES (?)", [(1,)])

    inner_cursor.close.assert_called_once()


async def test_execute_suppresses_close_failure_preserves_primary() -> None:
    """Primary execute error propagates; secondary close failure is suppressed."""
    inner_cursor = MagicMock()
    inner_cursor.execute = MagicMock(side_effect=OperationalError("primary"))
    inner_cursor.close = MagicMock(side_effect=OperationalError("secondary close failure"))

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(OperationalError, match="primary"):
        await greenlet_spawn(cur.execute, "SELECT 1")

    inner_cursor.close.assert_called_once()


async def test_executemany_suppresses_close_failure_preserves_primary() -> None:
    inner_cursor = MagicMock()
    inner_cursor.executemany = MagicMock(side_effect=OperationalError("primary emy"))
    inner_cursor.close = MagicMock(side_effect=OperationalError("secondary close failure"))

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(OperationalError, match="primary emy"):
        await greenlet_spawn(cur.executemany, "INSERT INTO t VALUES (?)", [(1,)])

    inner_cursor.close.assert_called_once()


async def test_execute_close_suppress_does_not_swallow_keyboard_interrupt() -> None:
    """``KeyboardInterrupt`` from ``cursor.close()`` must propagate: the suppress
    tuple is ``(Exception, asyncio.CancelledError)`` only, not BaseException."""
    inner_cursor = MagicMock()
    inner_cursor.execute = MagicMock(side_effect=OperationalError("body raise"))
    inner_cursor.close = MagicMock(side_effect=KeyboardInterrupt())

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(KeyboardInterrupt):
        await greenlet_spawn(cur.execute, "SELECT 1")


async def test_executemany_close_suppress_does_not_swallow_system_exit() -> None:
    """Symmetric: ``SystemExit`` from ``cursor.close()`` also propagates."""
    inner_cursor = MagicMock()
    inner_cursor.executemany = MagicMock(side_effect=OperationalError("body raise emy"))
    inner_cursor.close = MagicMock(side_effect=SystemExit())

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(SystemExit):
        await greenlet_spawn(cur.executemany, "INSERT INTO t VALUES (?)", [(1,)])
