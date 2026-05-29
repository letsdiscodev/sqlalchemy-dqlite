"""Pin ``AsyncAdaptedCursor.arraysize``: PEP 249 default of 1, and its
deque-only semantic. dqlite delivers the full result set in one RTT, so
``arraysize`` governs only ``fetchmany`` batch size, never wire prefetch.
"""

from __future__ import annotations

from collections import deque
from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor() -> AsyncAdaptedCursor:
    cur = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
    cur._adapt_connection = MagicMock()
    cur._connection = MagicMock()
    cur.description = None
    cur.rowcount = -1
    cur.lastrowid = None
    cur._arraysize = 1
    cur._rows = deque()
    cur._closed = False
    return cur


def test_adapter_arraysize_default_matches_pep249() -> None:
    """PEP 249 §6.1.2: ``arraysize`` defaults to 1."""
    cur = _make_cursor()
    assert cur.arraysize == 1


def test_adapter_arraysize_governs_deque_pop_not_wire_prefetch() -> None:
    """``arraysize`` sizes ``fetchmany``'s pop from the pre-drained deque,
    triggering no further fetches."""
    cur = _make_cursor()
    rows: list[tuple[Any, ...]] = [(i,) for i in range(10)]
    cur._rows = deque(rows)

    cur.arraysize = 3
    first = cur.fetchmany()
    second = cur.fetchmany()
    third = cur.fetchmany()
    fourth = cur.fetchmany()

    assert first == rows[0:3]
    assert second == rows[3:6]
    assert third == rows[6:9]
    assert fourth == rows[9:10]
    # Connection-level cursor untouched: no wire RTT from arraysize or fetchmany.
    cur._connection.cursor.assert_not_called()
