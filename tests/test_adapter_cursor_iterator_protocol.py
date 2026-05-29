"""``AsyncAdaptedCursor`` iterator protocol: ``iter(cursor)`` returns self so
``for`` and ``next()`` share one row buffer (a fresh-generator ``__iter__`` would not)."""

from __future__ import annotations

import collections
import collections.abc

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor(rows: list[tuple[int, ...]]) -> AsyncAdaptedCursor:
    cur = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
    cur._rows = collections.deque(rows)
    cur._closed = False
    return cur


def test_iter_returns_self() -> None:
    cur = _make_cursor([(1,), (2,), (3,)])
    assert iter(cur) is cur


def test_cursor_is_registered_as_iterator() -> None:
    cur = _make_cursor([])
    assert isinstance(cur, collections.abc.Iterator)


def test_for_loop_and_next_share_state() -> None:
    cur = _make_cursor([(1,), (2,), (3,)])
    first = next(cur)
    assert first == (1,)
    rest = list(cur)
    assert rest == [(2,), (3,)]


def test_empty_iteration_raises_stop_iteration() -> None:
    cur = _make_cursor([])
    import pytest

    with pytest.raises(StopIteration):
        next(cur)
