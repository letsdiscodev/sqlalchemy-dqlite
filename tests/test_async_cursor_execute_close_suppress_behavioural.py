"""Pin: ``AsyncAdaptedCursor.execute`` / ``executemany`` cursor-close
``finally`` is leak-preventing AND narrow-suppress (Exception +
CancelledError, NOT BaseException).

The existing ``_has_finally_with_close`` AST helper is structural only
— it returns True for any ``.close()`` call in any ``Try.finalbody``,
without verifying the close is on the cursor, the suppress is narrow,
or the close failure is actually absorbed at runtime.

These pins drive the production frame end-to-end:

1. ``execute``/``executemany`` raise → cursor.close() runs (leak
   prevention).
2. ``execute`` raises primary AND close raises secondary → primary
   propagates, secondary suppressed (exception preservation).
3. ``cursor.close()`` raises ``KeyboardInterrupt`` → KI propagates
   (narrow-suppress discipline; ``BaseException`` is NOT in the
   suppression set).
"""

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
    """Construct a cursor whose underlying ``AsyncConnection.cursor()``
    returns ``inner_cursor``."""
    adapter: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner_conn = MagicMock()
    inner_conn.cursor.return_value = inner_cursor
    adapter._connection = inner_conn
    return AsyncAdaptedCursor(adapter)


async def test_execute_closes_cursor_even_on_execute_raise() -> None:
    """Leak prevention: execute body raise → underlying cursor.close
    runs."""
    inner_cursor = MagicMock()
    # Make execute (which our adapter awaits) raise on the underlying.
    # The adapter wraps with await_only; an async MagicMock isn't
    # needed because await_only resolves the value if it has no
    # __await__ — instead we use a MagicMock side_effect that raises
    # synchronously.
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
    """Exception-preservation: when execute raises primary AND
    cursor.close raises secondary (transport-class), the primary
    propagates and the secondary is suppressed."""
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
    """Narrow-suppress: ``KeyboardInterrupt`` raised inside
    ``cursor.close()`` MUST propagate. ``BaseException`` is
    deliberately NOT in the suppress tuple — the stdlib
    ``contextlib.suppress`` docs flag wider suppress as an anti-
    pattern, and the project-wide narrow-suppress discipline pins
    ``(Exception, asyncio.CancelledError)`` only."""
    inner_cursor = MagicMock()
    inner_cursor.execute = MagicMock(side_effect=OperationalError("body raise"))
    inner_cursor.close = MagicMock(side_effect=KeyboardInterrupt())

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(KeyboardInterrupt):
        await greenlet_spawn(cur.execute, "SELECT 1")


async def test_executemany_close_suppress_does_not_swallow_system_exit() -> None:
    """Symmetric narrow-suppress: ``SystemExit`` from
    ``cursor.close()`` also propagates."""
    inner_cursor = MagicMock()
    inner_cursor.executemany = MagicMock(side_effect=OperationalError("body raise emy"))
    inner_cursor.close = MagicMock(side_effect=SystemExit())

    cur = _make_cursor_with_inner(inner_cursor)

    with pytest.raises(SystemExit):
        await greenlet_spawn(cur.executemany, "INSERT INTO t VALUES (?)", [(1,)])
