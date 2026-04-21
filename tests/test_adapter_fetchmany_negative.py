"""AsyncAdaptedCursor.fetchmany must reject negative size, matching
the PEP-249 contract enforced by the two sibling dqlitedbapi cursors
(ISSUE-82 and ISSUE-121).
"""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_fetchmany_negative_size_raises_programming_error() -> None:
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque([("a",), ("b",)])

    with pytest.raises(ProgrammingError, match="must be non-negative"):
        cur.fetchmany(-1)

    # Rows must not have been consumed on error.
    assert len(cur._rows) == 2


def test_fetchmany_zero_size_returns_empty() -> None:
    """Existing behaviour: size=0 returns []."""
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque([("a",)])
    assert list(cur.fetchmany(0)) == []
    assert len(cur._rows) == 1
