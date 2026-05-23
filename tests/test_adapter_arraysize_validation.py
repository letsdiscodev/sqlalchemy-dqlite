"""``AsyncAdaptedCursor.arraysize`` rejects ``bool`` and non-int.

Accepts any int including ``0`` and negatives, matching SA's
reference adapter (``sqlalchemy/connectors/asyncio.py``) and stdlib
``sqlite3.Cursor.arraysize`` (both unvalidated). PEP 249 §6.2 sets
no minimum. Bool / non-int rejection remains as a dqlite-specific
footgun-prevention with no SA-reference sibling.
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


@pytest.mark.parametrize("value", [0, -1, -100])
def test_accepts_zero_or_negative(value: int) -> None:
    """SA-reference / stdlib parity: zero and negatives pass through."""
    cur = _make_cursor()
    cur.arraysize = value
    assert cur.arraysize == value


@pytest.mark.parametrize("bad", [True, False])
def test_rejects_bool(bad: bool) -> None:
    cur = _make_cursor()
    with pytest.raises(ProgrammingError, match="must be an int"):
        cur.arraysize = bad


def test_rejects_non_int() -> None:
    cur = _make_cursor()
    with pytest.raises(ProgrammingError, match="must be an int"):
        cur.arraysize = "1"  # type: ignore[assignment]


def test_accepts_positive_int() -> None:
    cur = _make_cursor()
    cur.arraysize = 100
    assert cur.arraysize == 100
