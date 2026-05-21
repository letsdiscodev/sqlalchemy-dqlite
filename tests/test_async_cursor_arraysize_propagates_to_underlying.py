"""Pin: ``AsyncAdaptedCursor.execute`` and ``executemany`` propagate
``self._arraysize`` onto the freshly opened underlying ``AsyncCursor``
before the await.

Today the dbapi layer also buffers the full result in-memory and
``arraysize`` is deque-only, so this propagation is forward-compat
scaffolding — it makes the contract honourable for any future wire-
level prefetch tuning. The setter itself does NOT propagate (the
underlying cursor does not exist at setter time; opened fresh per
execute).
"""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_cursor_with_underlying() -> tuple[AsyncAdaptedCursor, MagicMock]:
    """Return an adapter cursor whose underlying ``AsyncCursor`` is a
    MagicMock that records ``arraysize`` writes."""
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
    """Replace ``await_only`` with a no-op so cursor.execute /
    executemany return immediately."""

    def _no_await(coro_or_value: object) -> object:
        # The underlying MagicMock returns a MagicMock from execute /
        # executemany; we just return it without driving the
        # coroutine.
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
    """The setter itself does NOT propagate — the underlying cursor
    does not exist at setter time. Propagation happens at execute
    time."""
    cur, inner = _make_cursor_with_underlying()
    # Sanity: underlying.cursor() has not been called yet.
    cur.arraysize = 99
    # No execute fired; the underlying cursor was never opened.
    assert not inner.method_calls


def test_fetchmany_still_returns_arraysize_sized_chunks_from_deque() -> None:
    """Regression: changing arraysize must still tune fetchmany's
    deque-pop batch size (the load-bearing semantic today)."""
    cur, _ = _make_cursor_with_underlying()
    cur._rows = deque([(1,), (2,), (3,), (4,), (5,)])
    cur.arraysize = 2
    assert cur.fetchmany() == [(1,), (2,)]
    assert cur.fetchmany() == [(3,), (4,)]
    assert cur.fetchmany() == [(5,)]
