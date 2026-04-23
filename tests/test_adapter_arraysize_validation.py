"""``AsyncAdaptedCursor.arraysize`` rejects invalid values.

Previously the bare slot let callers assign ``0`` or ``-1``, and
``fetchmany(size=None)`` fell back to ``self.arraysize``; with
``arraysize=0`` the slice ``min(0, len(self._rows)) = 0`` returned
``[]`` on every call, so the idiomatic
``while batch := cursor.fetchmany(): ...`` silently skipped the
entire result set. Match ``dqlitedbapi.Cursor.arraysize``'s setter:
reject ``bool``, non-int, and ``< 1``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor() -> AsyncAdaptedCursor:
    cur = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
    from collections import deque

    cur._rows = deque()
    cur._closed = False
    cur._arraysize = 1
    return cur


@pytest.mark.parametrize("bad", [0, -1, -100])
def test_rejects_zero_or_negative(bad: int) -> None:
    cur = _make_cursor()
    with pytest.raises(ProgrammingError, match=">= 1"):
        cur.arraysize = bad


@pytest.mark.parametrize("bad", [True, False])
def test_rejects_bool(bad: bool) -> None:
    cur = _make_cursor()
    with pytest.raises(ProgrammingError, match="positive integer"):
        cur.arraysize = bad


def test_rejects_non_int() -> None:
    cur = _make_cursor()
    with pytest.raises(ProgrammingError, match="positive integer"):
        cur.arraysize = "1"  # type: ignore[assignment]


def test_accepts_positive_int() -> None:
    cur = _make_cursor()
    cur.arraysize = 100
    assert cur.arraysize == 100
