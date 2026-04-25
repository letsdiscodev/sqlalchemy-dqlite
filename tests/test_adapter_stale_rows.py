"""AsyncAdaptedCursor must not leak rows from a previous
execute across an exception (including CancelledError)."""

import asyncio
from collections import deque
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest


class _FakeCursor:
    def __init__(self) -> None:
        self.description: Any = None
        self.rowcount = -1
        self.lastrowid: int | None = None

    async def execute(self, *a: Any, **kw: Any) -> None:
        pass

    async def executemany(self, *a: Any, **kw: Any) -> None:
        pass

    async def fetchall(self) -> list[Any]:
        return []

    async def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _sync_await(coro: Any) -> Any:
    """Run a coroutine synchronously; stand-in for ``await_only`` in
    tests so we don't need a greenlet context."""
    try:
        return asyncio.new_event_loop().run_until_complete(coro)
    except Exception:
        raise


def _reraise(error: BaseException) -> None:
    """Stand-in for AsyncAdaptedConnection._handle_exception in tests
    that don't go through a real adapt-connection."""
    raise error


def test_stale_rows_cleared_when_execute_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlalchemydqlite import aio as aio_mod
    from sqlalchemydqlite.aio import AsyncAdaptedCursor

    monkeypatch.setattr(aio_mod, "await_only", _sync_await)

    cursor = _FakeCursor()

    async def boom(*a: Any, **kw: Any) -> None:
        raise RuntimeError("bang")

    cursor.execute = boom  # type: ignore[method-assign]
    conn_adapter = MagicMock()
    conn_adapter._connection = _FakeConnection(cursor)
    conn_adapter._handle_exception = _reraise

    adapter = AsyncAdaptedCursor(conn_adapter)
    # Seed stale state simulating a prior successful execute.
    adapter.description = [("a", None, None, None, None, None, None)]
    adapter._rows = deque([(1,), (2,)])
    adapter.rowcount = 2
    adapter.lastrowid = 10

    with pytest.raises(RuntimeError):
        adapter.execute("SELECT 1")

    assert adapter.description is None
    assert adapter.rowcount == -1
    assert adapter.lastrowid is None
    assert list(adapter._rows) == []
    assert adapter.fetchone() is None  # Not the stale (1,).


def test_stale_rows_cleared_when_executemany_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlalchemydqlite import aio as aio_mod
    from sqlalchemydqlite.aio import AsyncAdaptedCursor

    monkeypatch.setattr(aio_mod, "await_only", _sync_await)

    cursor = _FakeCursor()

    async def boom(*a: Any, **kw: Any) -> None:
        raise RuntimeError("bang")

    cursor.executemany = boom  # type: ignore[method-assign]
    conn_adapter = SimpleNamespace(
        _connection=_FakeConnection(cursor),
        _handle_exception=_reraise,
    )

    adapter = AsyncAdaptedCursor(conn_adapter)  # type: ignore[arg-type]
    adapter.description = [("a", None, None, None, None, None, None)]
    adapter._rows = deque([(1,)])
    adapter.rowcount = 3
    adapter.lastrowid = 99

    with pytest.raises(RuntimeError):
        adapter.executemany("INSERT INTO t VALUES (?)", [[1], [2]])

    assert adapter.description is None
    assert adapter.rowcount == -1
    assert adapter.lastrowid is None
    assert list(adapter._rows) == []
