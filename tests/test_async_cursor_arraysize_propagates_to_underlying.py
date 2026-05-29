"""``execute``/``executemany`` propagate ``self._arraysize`` onto the freshly
opened underlying ``AsyncCursor`` before the await; the setter does not (no
underlying cursor exists at setter time). Forward-compat scaffolding today."""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_cursor_with_underlying() -> tuple[AsyncAdaptedCursor, MagicMock]:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner_conn = MagicMock()
    inner_cursor = MagicMock()
    inner_cursor.description = None
    inner_cursor.rowcount = 0
    inner_cursor.lastrowid = None
    inner_conn.cursor.return_value = inner_cursor
    adapter._connection = inner_conn
    cur = AsyncAdaptedCursor(adapter)
    return cur, inner_cursor


def _patch_await(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace ``await_only`` with a no-op so execute/executemany return at once."""

    def _no_await(coro_or_value: object) -> object:
        return None

    monkeypatch.setattr("sqlalchemydqlite.aio.await_only", _no_await)


def test_execute_propagates_arraysize_to_underlying_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur, inner = _make_cursor_with_underlying()
    cur._arraysize = 50
    _patch_await(monkeypatch)

    cur.execute("SELECT 1")

    assert inner.arraysize == 50


def test_executemany_propagates_arraysize_to_underlying_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur, inner = _make_cursor_with_underlying()
    cur._arraysize = 25
    _patch_await(monkeypatch)

    cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert inner.arraysize == 25


def test_arraysize_setter_does_not_touch_underlying() -> None:
    cur, inner = _make_cursor_with_underlying()
    cur.arraysize = 99
    assert not inner.method_calls


def test_fetchmany_still_returns_arraysize_sized_chunks_from_deque() -> None:
    """arraysize must still tune fetchmany's deque-pop batch size."""
    cur, _ = _make_cursor_with_underlying()
    cur._rows = deque([(1,), (2,), (3,), (4,), (5,)])
    cur.arraysize = 2
    assert cur.fetchmany() == [(1,), (2,)]
    assert cur.fetchmany() == [(3,), (4,)]
    assert cur.fetchmany() == [(5,)]
