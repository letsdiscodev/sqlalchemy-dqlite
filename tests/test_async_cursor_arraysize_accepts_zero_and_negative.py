"""Pin: ``AsyncAdaptedCursor.arraysize`` setter accepts ``0`` and
negative values, matching SA's reference adapter and stdlib
``sqlite3.Cursor.arraysize`` (both unvalidated). PEP 249 §6.2 sets
no minimum on arraysize.

Before this fix the setter rejected ``arraysize < 1`` with
``ProgrammingError``. Cross-driver porting code using
``cursor.arraysize = 0`` as "use the underlying default" hit a
gratuitous rejection that no other driver enforced.

``bool`` and non-int rejection is preserved as dqlite-specific
footgun-prevention with no SA-reference sibling.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor() -> AsyncAdaptedCursor:
    """Construct the async-adapted cursor without dialling. Only the
    arraysize setter is under test; the inner cursor's state is not
    exercised."""
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
    """Stdlib / SA-reference parity: zero is a legitimate input
    meaning "use the underlying default."""
    cur = _make_cursor()
    cur.arraysize = 0
    assert cur.arraysize == 0


def test_arraysize_negative_accepted() -> None:
    """Stdlib / SA-reference parity: negative values are passed
    through without rejection (PEP 249 §6.2 sets no minimum)."""
    cur = _make_cursor()
    cur.arraysize = -1
    assert cur.arraysize == -1


def test_arraysize_bool_still_rejected() -> None:
    """Bool footgun protection preserved -- the dqlite-specific
    safeguard against silent True/False coercion stays."""
    cur = _make_cursor()
    from dqlitedbapi.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError):
        cur.arraysize = True


def test_arraysize_non_int_still_rejected() -> None:
    cur = _make_cursor()
    from dqlitedbapi.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError):
        cur.arraysize = "10"  # type: ignore[assignment]
