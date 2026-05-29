"""AsyncAdaptedCursor.execute / executemany route errors through the parent
connection's _handle_exception hook (the single place to remap driver quirks)."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedCursor


class _FakeCursor:
    description: Any = None
    rowcount: int = -1
    lastrowid: int | None = None

    async def execute(self, *a: Any, **kw: Any) -> None:
        raise RuntimeError("driver-layer quirk")

    async def executemany(self, *a: Any, **kw: Any) -> None:
        raise RuntimeError("driver-layer quirk")

    async def fetchall(self) -> list[Any]:
        return []

    async def close(self) -> None:
        pass


class _FakeConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


def _sync_await(coro: Any) -> Any:
    return asyncio.new_event_loop().run_until_complete(coro)


def test_execute_routes_error_through_handle_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlalchemydqlite import aio as aio_mod

    monkeypatch.setattr(aio_mod, "await_only", _sync_await)

    captured: list[BaseException] = []

    def remap(error: BaseException) -> None:
        captured.append(error)
        raise ValueError("remapped") from error

    conn_adapter = MagicMock()
    conn_adapter._connection = _FakeConnection()
    conn_adapter._handle_exception = remap

    cur = AsyncAdaptedCursor(conn_adapter)
    with pytest.raises(ValueError, match="remapped") as ei:
        cur.execute("SELECT 1")

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert len(captured) == 1
    assert isinstance(captured[0], RuntimeError)


def test_executemany_routes_error_through_handle_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlalchemydqlite import aio as aio_mod

    monkeypatch.setattr(aio_mod, "await_only", _sync_await)

    captured: list[BaseException] = []

    def remap(error: BaseException) -> None:
        captured.append(error)
        raise error

    conn_adapter = MagicMock()
    conn_adapter._connection = _FakeConnection()
    conn_adapter._handle_exception = remap

    cur = AsyncAdaptedCursor(conn_adapter)
    cur._rows = deque([(1,)])  # exercise the reset
    with pytest.raises(RuntimeError):
        cur.executemany("INSERT INTO t VALUES (?)", [[1]])
    assert len(captured) == 1
