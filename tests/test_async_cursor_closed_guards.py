"""Pin the closed-cursor guard on AsyncAdaptedCursor.execute /
executemany, and pin the post-close state contract: ``close()`` clears
the result-set surface (``description`` / rows) but PRESERVES
``rowcount`` / ``lastrowid``.

Every other method on the adapter cursor (fetchone / fetchmany /
fetchall / setinputsizes / setoutputsize / callproc / nextset /
scroll) raises ``InterfaceError("cursor is closed")`` when ``_closed``.
``execute`` and ``executemany`` previously skipped the check — a
post-close execute silently ran against the underlying dbapi cursor
and the user only saw "cursor is closed" from the first fetch.

``close()`` clears ``description`` and the buffered rows (a closed
cursor cannot fetch) but keeps ``rowcount`` / ``lastrowid`` readable,
matching stdlib ``sqlite3.Cursor`` and the dbapi cursor so SA's Result
layer can read ``lastrowid`` after the cursor is closed.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import InterfaceError
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _make_cursor() -> AsyncAdaptedCursor:
    fake_connection = MagicMock()
    return AsyncAdaptedCursor(fake_connection)


def test_execute_on_closed_cursor_raises() -> None:
    cur = _make_cursor()
    cur.close()
    with pytest.raises(InterfaceError, match="closed"):
        cur.execute("SELECT 1")


def test_executemany_on_closed_cursor_raises() -> None:
    cur = _make_cursor()
    cur.close()
    with pytest.raises(InterfaceError, match="closed"):
        cur.executemany("INSERT INTO t (n) VALUES (?)", [(1,)])


def test_close_clears_result_set_but_preserves_rowcount_lastrowid() -> None:
    cur = _make_cursor()
    # Simulate post-execute state.
    cur.description = (("col", None, None, None, None, None, None),)
    cur.rowcount = 42
    cur.lastrowid = 7
    cur._rows.append((1,))
    cur.close()
    # Result-set surface cleared (a closed cursor cannot fetch)...
    assert cur.description is None
    assert len(cur._rows) == 0
    # ...but rowcount / lastrowid survive close, matching stdlib and the
    # dbapi cursor, so SA's Result layer can read lastrowid post-close.
    assert cur.rowcount == 42
    assert cur.lastrowid == 7
    assert cur._closed is True
