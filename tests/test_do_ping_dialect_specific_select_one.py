"""Pin: ``do_ping`` (sync and async) routes through
``self._dialect_specific_select_one`` rather than a hard-coded
``"SELECT 1"`` literal.

SA's ``DefaultDialect.do_ping`` (``engine/default.py:736-769``) calls
``cursor.execute(self._dialect_specific_select_one)``. The cached
property compiles ``select(1)`` against the dialect, so dialects can
inject a custom rendering by overriding the property (or, for tests,
monkeypatching it). Pinning the dispatch through the property protects
the override from drifting back to a literal that silently bypasses
test-harness injection and any future dqlite-specific rendering.
"""

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
