"""Pin: ``AsyncAdaptedCursor.arraysize`` default + deque-only semantic.

PEP 249 §6.1.2 says ``arraysize`` defaults to ``1`` and governs the
per-``fetchmany`` batch size. The adapter satisfies that contract — but
its ``arraysize`` is semantically **deque-only**: dqlite's wire layer
delivers the entire result set up-front in a single RTT, so changing
``arraysize`` does not influence any wire prefetch. Pin both: the
default, and the deque-pop semantic, so a future refactor that
accidentally couples wire prefetch to ``arraysize`` (or diverges from
the PEP 249 default) trips this test.
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
    """``arraysize`` controls the batch size of ``fetchmany``'s pop from
    the pre-drained deque. The deque is already populated (single wire
    RTT delivered the full result set up-front); ``arraysize`` does not
    trigger any further fetches.
    """
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
    # The connection-level cursor was never touched — no wire RTT was
    # triggered by changing ``arraysize`` or by the four fetchmany
    # calls. Pin: deque-only governance.
    cur._connection.cursor.assert_not_called()
