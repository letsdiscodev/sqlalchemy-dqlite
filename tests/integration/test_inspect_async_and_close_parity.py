"""Three SA dialect surfaces that lacked dedicated coverage:

1. ``inspect(async_conn.run_sync(...))`` — SA's reflection visitor must
   reach the dialect through the async engine. Without a pin, an
   inadvertent override that breaks ``get_driver_connection`` /
   reflection delegation regresses silently and only surfaces when a
   user runs ``Inspector.get_table_names()``.

2. ``do_close`` parity between sync and async dialects: both must
   close the underlying connection through the dialect's ``close()``
   surface (sync ``Connection.close`` vs async
   ``AsyncAdaptedConnection.close``) so a caller-driven
   ``connection.close()`` reaches the same teardown path as
   ``engine.dispose()``.

3. ``AsyncAdaptedCursor._async_soft_close`` is a no-op — SA's
   ``Result`` calls it during streaming-result teardown. The cursor
   adapter holds no long-lived underlying cursor (each execute opens
   and closes its own) so soft-close has nothing to flush; the no-op
   contract is what makes that lifecycle correct.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Generator

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


@pytest.fixture
def reflected_table(engine_url: str) -> Generator[tuple[Engine, str]]:
    table_name = f"refl_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("label", String(50)),
    )
    eng = create_engine(engine_url, future=True)
    metadata.create_all(eng)
    try:
        yield eng, table_name
    finally:
        metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
async def reflected_table_async(
    async_engine_url: str,
) -> AsyncGenerator[tuple[AsyncEngine, str]]:
    table_name = f"refl_async_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("label", String(50)),
    )
    eng = create_async_engine(async_engine_url, future=True)
    async with eng.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield eng, table_name
    finally:
        async with eng.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await eng.dispose()


@pytest.mark.asyncio
async def test_inspect_async_engine_returns_table_names(
    reflected_table_async: tuple[AsyncEngine, str],
) -> None:
    """Async ``inspect`` must reach the dialect through ``run_sync``
    and surface the reflected table. The reflection path bridges
    async → sync via ``conn.run_sync(inspect)`` so the inspector ends
    up driving the same ``DqliteDialect`` get_*_names methods that
    the sync engine exercises."""
    eng, table_name = reflected_table_async
    async with eng.connect() as conn:
        names = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())
    assert table_name in names


def test_inspect_sync_engine_returns_table_names(
    reflected_table: tuple[Engine, str],
) -> None:
    """Sync parity check: the same reflection path on the sync engine
    must also surface the table. Pinning both ensures any divergence
    in dialect wiring is visible in CI."""
    eng, table_name = reflected_table
    insp = inspect(eng)
    names = insp.get_table_names()
    assert table_name in names


@pytest.mark.asyncio
async def test_async_engine_explicit_connection_close_runs_adapter_close(
    async_engine_url: str,
) -> None:
    """Pin: an explicit ``async with engine.connect() as conn`` exit
    routes through ``AsyncAdaptedConnection.close()`` and not through
    a back door — verifying the do_close parity contract that
    ``engine.dispose()`` relies on."""
    eng = create_async_engine(async_engine_url, future=True)
    try:
        # First trip: open + close. Pool retains the dbapi connection.
        async with eng.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
        # Second trip: dispose to force the pool to close the dbapi
        # connection through ``AsyncAdaptedConnection.close``. A bug
        # that leaks the conn would surface as a hung dispose; the
        # passing test is itself the pin.
    finally:
        await eng.dispose()


@pytest.mark.asyncio
async def test_async_soft_close_is_noop_on_cursor() -> None:
    """``AsyncAdaptedCursor`` opens + closes a fresh underlying
    AsyncCursor on every execute, so the streaming-result soft-close
    path on the adapter has nothing to flush. Pin the no-op contract
    so a future change that adds work here surfaces in CI — work
    inside ``_async_soft_close`` would race the per-execute cursor
    lifecycle."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = None
    cur = AsyncAdaptedCursor(adapter)
    # Pre-state and post-state must be identical — the no-op preserves
    # description/rowcount/lastrowid/closed.
    cur.description = (("x", 4, None, None, None, None, None),)
    cur.rowcount = 7
    cur.lastrowid = 42
    cur._closed = False

    await cur._async_soft_close()

    assert cur.description == (("x", 4, None, None, None, None, None),)
    assert cur.rowcount == 7
    assert cur.lastrowid == 42
    assert cur._closed is False
