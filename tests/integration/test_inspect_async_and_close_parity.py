"""Coverage for three SA dialect surfaces: async reflection via run_sync,
do_close parity (sync vs async), and the AsyncAdaptedCursor soft-close no-op."""

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
    """Async ``inspect`` reaches the dialect via ``run_sync`` and surfaces
    the reflected table, driving the same DqliteDialect get_*_names methods."""
    eng, table_name = reflected_table_async
    async with eng.connect() as conn:
        names = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())
    assert table_name in names


def test_inspect_sync_engine_returns_table_names(
    reflected_table: tuple[Engine, str],
) -> None:
    """Sync parity: the same reflection path surfaces the table on the sync engine."""
    eng, table_name = reflected_table
    insp = inspect(eng)
    names = insp.get_table_names()
    assert table_name in names


@pytest.mark.asyncio
async def test_async_engine_explicit_connection_close_runs_adapter_close(
    async_engine_url: str,
) -> None:
    """An explicit ``engine.connect()`` exit routes through
    ``AsyncAdaptedConnection.close()`` — the do_close parity contract."""
    eng = create_async_engine(async_engine_url, future=True)
    try:
        async with eng.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
        # dispose() forces the pool to close the dbapi conn; a leak would hang here.
    finally:
        await eng.dispose()


@pytest.mark.asyncio
async def test_async_soft_close_is_noop_on_cursor() -> None:
    """soft-close is a no-op: each execute opens+closes its own cursor, so adding
    work in ``_async_soft_close`` would race the per-execute cursor lifecycle."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = None
    cur = AsyncAdaptedCursor(adapter)
    cur.description = (("x", 4, None, None, None, None, None),)
    cur.rowcount = 7
    cur.lastrowid = 42
    cur._closed = False

    await cur._async_soft_close()

    assert cur.description == (("x", 4, None, None, None, None, None),)
    assert cur.rowcount == 7
    assert cur.lastrowid == 42
    assert cur._closed is False
