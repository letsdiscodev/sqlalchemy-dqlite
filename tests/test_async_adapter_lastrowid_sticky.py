"""Pin: ``AsyncAdaptedCursor.lastrowid`` is sticky across non-INSERT
executes, like stdlib ``sqlite3.Cursor.lastrowid``. The adapter opens a
fresh underlying cursor per ``execute()``, so it must write lastrowid only
when the underlying value is non-None, else an UPDATE/SELECT clobbers it.
"""

from __future__ import annotations

from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    adapter._connection = inner
    return adapter


def _make_underlying(*, lastrowid: int | None, description: object = None) -> MagicMock:
    cur = MagicMock()
    cur.execute = AsyncMock()
    cur.executemany = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[])
    cur.description = description
    cur.rowcount = 1 if description is None else -1
    cur.lastrowid = lastrowid
    cur.close = AsyncMock()
    return cur


@pytest.mark.asyncio
async def test_lastrowid_sticky_across_non_insert_execute() -> None:
    """INSERT then UPDATE keeps the INSERT's lastrowid."""
    adapter = _make_adapter()
    insert_underlying = _make_underlying(lastrowid=5)
    update_underlying = _make_underlying(lastrowid=None)
    adapter._connection.cursor = MagicMock(side_effect=[insert_underlying, update_underlying])

    cursor = AsyncAdaptedCursor(adapter)
    cursor._rows = deque()

    await greenlet_spawn(cursor.execute, "INSERT INTO t(v) VALUES (?)", (1,))
    assert cursor.lastrowid == 5
    await greenlet_spawn(cursor.execute, "UPDATE t SET v=v+1 WHERE id=5")
    assert cursor.lastrowid == 5, (
        f"lastrowid must remain sticky after non-INSERT execute; got {cursor.lastrowid}"
    )


@pytest.mark.asyncio
async def test_lastrowid_sticky_across_select_execute() -> None:
    """INSERT then SELECT keeps the INSERT's lastrowid."""
    adapter = _make_adapter()
    insert_underlying = _make_underlying(lastrowid=7)
    select_description = (("col0", None, None, None, None, None, None),)
    select_underlying = _make_underlying(lastrowid=None, description=select_description)
    select_underlying.fetchall = AsyncMock(return_value=[(1, "a")])
    adapter._connection.cursor = MagicMock(side_effect=[insert_underlying, select_underlying])

    cursor = AsyncAdaptedCursor(adapter)
    cursor._rows = deque()

    await greenlet_spawn(cursor.execute, "INSERT INTO t(v) VALUES (?)", (1,))
    assert cursor.lastrowid == 7
    await greenlet_spawn(cursor.execute, "SELECT * FROM t")
    assert cursor.lastrowid == 7, (
        f"lastrowid must remain sticky after SELECT; got {cursor.lastrowid}"
    )


@pytest.mark.asyncio
async def test_lastrowid_updates_on_subsequent_insert() -> None:
    """The second INSERT's rowid replaces the first."""
    adapter = _make_adapter()
    first_insert = _make_underlying(lastrowid=5)
    second_insert = _make_underlying(lastrowid=6)
    adapter._connection.cursor = MagicMock(side_effect=[first_insert, second_insert])

    cursor = AsyncAdaptedCursor(adapter)
    cursor._rows = deque()

    await greenlet_spawn(cursor.execute, "INSERT INTO t(v) VALUES (?)", (1,))
    assert cursor.lastrowid == 5
    await greenlet_spawn(cursor.execute, "INSERT INTO t(v) VALUES (?)", (2,))
    assert cursor.lastrowid == 6


@pytest.mark.asyncio
async def test_executemany_lastrowid_also_sticky() -> None:
    """executemany lastrowid is sticky too."""
    adapter = _make_adapter()
    insert_em = _make_underlying(lastrowid=42)
    update_em = _make_underlying(lastrowid=None)
    adapter._connection.cursor = MagicMock(side_effect=[insert_em, update_em])

    cursor = AsyncAdaptedCursor(adapter)
    cursor._rows = deque()

    await greenlet_spawn(cursor.executemany, "INSERT INTO t(v) VALUES (?)", [(1,), (2,)])
    assert cursor.lastrowid == 42
    await greenlet_spawn(cursor.executemany, "UPDATE t SET v=v+1 WHERE id=?", [(1,)])
    assert cursor.lastrowid == 42
