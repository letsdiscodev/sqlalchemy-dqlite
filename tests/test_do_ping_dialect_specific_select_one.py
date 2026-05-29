"""Pin: ``do_ping`` (sync/async) dispatches through ``_dialect_specific_select_one``."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_sync_do_ping_dispatches_through_dialect_specific_select_one(
    monkeypatch: Any,
) -> None:
    dialect = DqliteDialect()
    sentinel_sql = "SELECT 1 -- ping"
    monkeypatch.setattr(DqliteDialect, "_dialect_specific_select_one", sentinel_sql, raising=False)

    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)

    assert dialect.do_ping(conn) is True
    cursor.execute.assert_called_once_with(sentinel_sql)


async def test_async_do_ping_dispatches_through_dialect_specific_select_one(
    monkeypatch: Any,
) -> None:
    dialect = DqliteDialect_aio()
    sentinel_sql = "SELECT 1 -- ping-async"
    monkeypatch.setattr(
        DqliteDialect_aio,
        "_dialect_specific_select_one",
        sentinel_sql,
        raising=False,
    )

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(1,))
    cursor.close = AsyncMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is True
    cursor.execute.assert_awaited_once_with(sentinel_sql)
