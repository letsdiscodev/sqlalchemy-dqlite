"""``AsyncAdaptedCursor`` implements the PEP 234 iterator protocol.

``iter(cursor)`` must return the cursor itself so that ``for row in
cursor`` and ``next(cursor)`` share the same underlying state. The
previous generator-based ``__iter__`` returned a fresh generator per
``iter()`` call and pulled rows directly out of ``_rows`` via
``popleft()``, while ``__next__`` went through ``fetchone()`` —
leaving two incompatible iteration paths reading the same row buffer.
"""

from __future__ import annotations

import collections
import collections.abc

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor(rows: list[tuple[int, ...]]) -> AsyncAdaptedCursor:
    # ``AsyncAdaptedCursor`` is normally instantiated via
    # ``AsyncAdaptedConnection.cursor()``; we don't need the connection
    # for iteration-protocol tests — we just need the _rows deque.
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
