"""``AsyncAdaptedCursor.arraysize`` setter accepts ``0`` and negative values
(PEP 249 §6.2 sets no minimum); ``bool``/non-int are still rejected as a
dqlite-specific footgun guard."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor() -> AsyncAdaptedCursor:
    from collections import deque

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


def test_arraysize_zero_accepted() -> None:
    cur = _make_cursor()
    cur.arraysize = 0
    assert cur.arraysize == 0


def test_arraysize_negative_accepted() -> None:
    cur = _make_cursor()
    cur.arraysize = -1
    assert cur.arraysize == -1


def test_arraysize_bool_still_rejected() -> None:
    cur = _make_cursor()
    from dqlitedbapi.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError):
        cur.arraysize = True


def test_arraysize_non_int_still_rejected() -> None:
    cur = _make_cursor()
    from dqlitedbapi.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError):
        cur.arraysize = "10"  # type: ignore[assignment]
