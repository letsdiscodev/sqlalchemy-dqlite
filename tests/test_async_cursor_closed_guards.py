"""Closed-cursor guard on execute/executemany, plus the post-close state
contract: ``close()`` clears the result-set surface (``description``/rows) but
preserves ``rowcount``/``lastrowid`` so SA's Result layer can read lastrowid."""

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
    cur.description = (("col", None, None, None, None, None, None),)
    cur.rowcount = 42
    cur.lastrowid = 7
    cur._rows.append((1,))
    cur.close()
    assert cur.description is None
    assert len(cur._rows) == 0
    assert cur.rowcount == 42
    assert cur.lastrowid == 7
    assert cur._closed is True
