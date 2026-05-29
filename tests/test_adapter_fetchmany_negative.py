"""``AsyncAdaptedCursor.fetchmany(size < 0)`` returns all remaining rows (stdlib parity)."""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_fetchmany_negative_size_returns_all_remaining() -> None:
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque([("a",), ("b",), ("c",)])

    rows = list(cur.fetchmany(-1))
    assert rows == [("a",), ("b",), ("c",)]
    assert len(cur._rows) == 0


def test_fetchmany_negative_size_on_empty_deque_returns_empty() -> None:
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque()

    assert list(cur.fetchmany(-1)) == []


def test_fetchmany_zero_size_returns_empty() -> None:
    """size=0 returns [] without draining the deque (distinct from size < 0)."""
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque([("a",)])
    assert list(cur.fetchmany(0)) == []
    assert len(cur._rows) == 1
