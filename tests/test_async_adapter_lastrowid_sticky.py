"""Pin: ``AsyncAdaptedCursor.lastrowid`` is sticky across non-INSERT
executes on the same adapter cursor — matching the dbapi-layer
``Cursor`` contract and the stdlib ``sqlite3.Cursor.lastrowid``
contract.

The async adapter opens a fresh underlying ``AsyncCursor`` per
``execute()`` (the dbapi-layer stickiness lives on the dbapi cursor,
not on the adapter), so we must preserve the prior INSERT's rowid
across non-INSERT executes by writing only when the underlying
cursor reports a non-None value (i.e. an INSERT/REPLACE actually
ran). Without this, a subsequent UPDATE / DELETE / DDL / SELECT on
the same adapter cursor silently clobbers ``lastrowid`` to None and
the sync vs async dialects diverge for the same sequence of
statements.
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
    """An INSERT followed by an UPDATE on the same adapter cursor
    keeps the INSERT's lastrowid (sticky)."""
    adapter = _make_adapter()
    insert_underlying = _make_underlying(lastrowid=5)
    update_underlying = _make_underlying(lastrowid=None)
    adapter._connection.cursor = MagicMock(side_effect=[insert_underlying, update_underlying])

    cursor = AsyncAdaptedCursor(adapter)
    cursor._rows = deque()

    await greenlet_spawn(cursor.execute, "INSERT INTO t(v) VALUES (?)", (1,))
    assert cursor.lastrowid == 5
    await greenlet_spawn(cursor.execute, "UPDATE t SET v=v+1 WHERE id=5")
    # After UPDATE — the lastrowid must remain 5, not be clobbered to None.
    assert cursor.lastrowid == 5, (
        f"lastrowid must remain sticky after non-INSERT execute; got {cursor.lastrowid}"
    )


@pytest.mark.asyncio
async def test_lastrowid_sticky_across_select_execute() -> None:
    """INSERT followed by SELECT on the same adapter cursor — sticky."""
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
    """Two consecutive INSERTs on the same adapter cursor — second
    INSERT's rowid replaces the first."""
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
    """The same sticky discipline applies to executemany."""
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
