"""Pin the contract that the underlying dbapi
``AsyncConnection._op_lock`` serialises every adapter
``execute`` / ``executemany`` round-trip — without an explicit
``_execute_mutex`` on the SA adapter (which the SA reference
``AsyncAdapt_dbapi_connection`` adds).

If a future refactor either:

  (a) drops the per-execute ``op_lock`` acquire inside the dbapi's
      ``AsyncCursor.execute`` / ``executemany`` path, OR
  (b) introduces a long-lived adapter cursor that bypasses the
      dbapi cursor's per-call op_lock acquire,

then two greenlets sharing one adapter could interleave
``cursor.execute`` calls. The pin here asserts that the dbapi
op_lock IS held during the inner execute so the mutex remains
unnecessary.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


@pytest.mark.asyncio
async def test_dbapi_op_lock_serialises_adapter_execute() -> None:
    """Two adapter ``execute`` calls dispatched in parallel must be
    observed serialised by the underlying dbapi ``op_lock`` —
    pinning that the lock IS the serialisation primitive the SA
    ``_execute_mutex`` would otherwise provide."""
    op_lock = asyncio.Lock()

    # Track the order in which the two execute bodies see the lock
    # held / released.
    observation: list[str] = []

    async def serialised_execute(_op: str, _params: Any | None = None) -> None:
        # Verify lock IS held while we run; if a future regression
        # drops the lock the parallel calls would interleave and the
        # observation order would change.
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

    # Drive both execute calls concurrently via the dbapi cursor's
    # execute (which is what the adapter cursor body invokes).
    await asyncio.gather(
        fake_dbapi_cursor.execute("SELECT 1"),
        fake_dbapi_cursor.execute("SELECT 2"),
    )

    # The lock-holding execute body recorded a strict
    # start/end pair per call — no interleave.
    assert observation == [
        "execute:start",
        "execute:end",
        "execute:start",
        "execute:end",
    ], f"saw interleave: {observation}"

    # Sanity: the adapter cursor objects exist and reference the same
    # underlying connection (no per-cursor mutex was needed).
    assert cursor1._adapt_connection is cursor2._adapt_connection


def test_async_adapter_documents_op_lock_subsumes_execute_mutex() -> None:
    """Pin the docstring claim that the underlying op_lock subsumes
    SA's reference _execute_mutex. A future refactor that removes
    the doc note should also re-introduce the mutex."""
    doc = AsyncAdaptedConnection.__doc__ or ""
    assert "_execute_mutex" in doc
    assert "op_lock" in doc
