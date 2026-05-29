"""Pin: ``do_ping`` returns ``False`` when ``_async_ping`` raises a RuntimeError outside the
known loop-state phrasings, so an un-remapped RuntimeError never escapes and pins a dead slot."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


@pytest.mark.parametrize(
    "message",
    [
        "no running event loop",
        "Task got bad yield: <something>",
        "synthetic loop-state error not in remap list",
    ],
)
async def test_do_ping_runtime_error_returns_false(message: str) -> None:
    """Unmapped RuntimeError on the ping path returns False, not an escape."""
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=RuntimeError(message))
    cursor.close = AsyncMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False
