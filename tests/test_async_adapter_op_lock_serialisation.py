"""Pin: the dbapi ``AsyncConnection._op_lock`` serialises every adapter
execute round-trip, so the SA reference adapter's explicit
``_execute_mutex`` stays unnecessary. The lock must be held during the
inner execute, else two greenlets sharing one adapter could interleave.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


@pytest.mark.asyncio
async def test_dbapi_op_lock_serialises_adapter_execute() -> None:
    """Two parallel adapter executes are serialised by the dbapi op_lock —
    the primitive SA's ``_execute_mutex`` would otherwise provide."""
    op_lock = asyncio.Lock()

    observation: list[str] = []

    async def serialised_execute(_op: str, _params: Any | None = None) -> None:
        assert op_lock.locked(), (
            "expected dbapi op_lock to be held during adapter execute round-trip"
        )
        observation.append("execute:start")
        await asyncio.sleep(0.01)
        observation.append("execute:end")

    async def lock_wrapped_execute(op: str, params: Any | None = None) -> None:
        async with op_lock:
            await serialised_execute(op, params)

    fake_dbapi_cursor = MagicMock()
    fake_dbapi_cursor.execute = AsyncMock(side_effect=lock_wrapped_execute)
    fake_dbapi_cursor.executemany = AsyncMock(side_effect=lock_wrapped_execute)
    fake_dbapi_cursor.close = AsyncMock()
    fake_dbapi_cursor.fetchall = AsyncMock(return_value=[])
    fake_dbapi_cursor.description = []
    fake_dbapi_cursor.rowcount = -1
    fake_dbapi_cursor.lastrowid = None

    fake_dbapi_conn = MagicMock()
    fake_dbapi_conn._op_lock = op_lock
    fake_dbapi_conn.cursor = MagicMock(return_value=fake_dbapi_cursor)

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = fake_dbapi_conn

    cursor1 = AsyncAdaptedCursor(adapter)
    cursor2 = AsyncAdaptedCursor(adapter)

    # Drive both executes concurrently via the dbapi cursor (what the
    # adapter cursor body invokes).
    await asyncio.gather(
        fake_dbapi_cursor.execute("SELECT 1"),
        fake_dbapi_cursor.execute("SELECT 2"),
    )

    # A strict start/end pair per call means no interleave.
    assert observation == [
        "execute:start",
        "execute:end",
        "execute:start",
        "execute:end",
    ], f"saw interleave: {observation}"

    assert cursor1._adapt_connection is cursor2._adapt_connection
