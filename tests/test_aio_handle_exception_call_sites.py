"""Pin: every call site that wraps ``await_only`` routes the
loop-mismatch ``RuntimeError`` through ``_handle_exception`` so the
remap fires regardless of which method tripped it.

Round 2 added the remap on commit/rollback/execute/executemany; Round
3 closed the ``close()`` gap. Without per-site coverage a future
refactor that splits a call out of the ``try/except BaseException``
envelope (e.g., adding an early-return fast path or moving the
``await_only`` inside a helper) silently regresses the classification
on that path, and SA's pool stops invalidating slots when the wrong
loop touches them.

Each test runs through SA's ``greenlet_spawn`` so ``await_only`` works.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor

_LOOP_MISMATCH_MSG = "Future ... attached to a different loop"


def _make_adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    adapter._connection = inner
    return adapter


@pytest.mark.asyncio
async def test_commit_remaps_loop_mismatch_runtimeerror() -> None:
    adapter = _make_adapter()
    adapter._connection.commit = AsyncMock(side_effect=RuntimeError(_LOOP_MISMATCH_MSG))

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        await greenlet_spawn(adapter.commit)


@pytest.mark.asyncio
async def test_rollback_remaps_loop_mismatch_runtimeerror() -> None:
    adapter = _make_adapter()
    adapter._connection.rollback = AsyncMock(side_effect=RuntimeError(_LOOP_MISMATCH_MSG))

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        await greenlet_spawn(adapter.rollback)


@pytest.mark.asyncio
async def test_commit_propagates_other_runtimeerrors_unchanged() -> None:
    adapter = _make_adapter()
    adapter._connection.commit = AsyncMock(side_effect=RuntimeError("server fault"))

    with pytest.raises(RuntimeError, match="server fault"):
        await greenlet_spawn(adapter.commit)


@pytest.mark.asyncio
async def test_rollback_propagates_other_runtimeerrors_unchanged() -> None:
    adapter = _make_adapter()
    adapter._connection.rollback = AsyncMock(side_effect=RuntimeError("server fault"))

    with pytest.raises(RuntimeError, match="server fault"):
        await greenlet_spawn(adapter.rollback)


def _make_cursor_with_failing_inner(method: str, exc: BaseException) -> AsyncAdaptedCursor:
    """Build an ``AsyncAdaptedCursor`` whose underlying cursor raises
    ``exc`` from ``method`` (one of ``execute``/``executemany``).
    """
    adapter = _make_adapter()
    inner_cursor = MagicMock()
    setattr(inner_cursor, method, AsyncMock(side_effect=exc))
    inner_cursor.close = AsyncMock()
    adapter._connection.cursor = MagicMock(return_value=inner_cursor)
    return AsyncAdaptedCursor(adapter)


@pytest.mark.asyncio
async def test_execute_remaps_loop_mismatch_runtimeerror() -> None:
    cursor = _make_cursor_with_failing_inner("execute", RuntimeError(_LOOP_MISMATCH_MSG))

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        await greenlet_spawn(cursor.execute, "SELECT 1", ())


@pytest.mark.asyncio
async def test_executemany_remaps_loop_mismatch_runtimeerror() -> None:
    cursor = _make_cursor_with_failing_inner("executemany", RuntimeError(_LOOP_MISMATCH_MSG))

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        await greenlet_spawn(cursor.executemany, "INSERT INTO t VALUES (?)", [(1,)])


@pytest.mark.asyncio
async def test_execute_propagates_other_runtimeerrors_unchanged() -> None:
    cursor = _make_cursor_with_failing_inner("execute", RuntimeError("driver bug"))

    with pytest.raises(RuntimeError, match="driver bug"):
        await greenlet_spawn(cursor.execute, "SELECT 1", ())


@pytest.mark.asyncio
async def test_executemany_propagates_other_runtimeerrors_unchanged() -> None:
    cursor = _make_cursor_with_failing_inner("executemany", RuntimeError("driver bug"))

    with pytest.raises(RuntimeError, match="driver bug"):
        await greenlet_spawn(cursor.executemany, "INSERT INTO t VALUES (?)", [(1,)])
