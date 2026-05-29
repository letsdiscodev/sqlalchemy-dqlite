"""Pin SAVEPOINT / RELEASE / ROLLBACK_TO_SAVEPOINT end-to-end against dqlite,
through the full SA -> dbapi -> client -> wire -> server path.
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
    insert,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture
def savepoint_table(engine_url: str) -> Generator[tuple[Engine, Table]]:
    """Per-test engine + uniquely-named table to avoid cross-run collisions."""
    table_name = f"sp_basic_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("label", String(50)),
    )
    eng = create_engine(engine_url, future=True)
    metadata.create_all(eng)
    try:
        yield eng, table
    finally:
        metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
async def async_savepoint_table(
    async_engine_url: str,
) -> AsyncGenerator[tuple[AsyncEngine, Table]]:
    table_name = f"sp_basic_async_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("label", String(50)),
    )
    eng = create_async_engine(async_engine_url, future=True)
    async with eng.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield eng, table
    finally:
        async with eng.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await eng.dispose()


@pytest.mark.integration
class TestSavepointBasic:
    def test_rollback_savepoint_preserves_outer_tx(
        self, savepoint_table: tuple[Engine, Table]
    ) -> None:
        engine, table = savepoint_table
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(insert(table).values(label="outer"))
                sp = conn.begin_nested()
                conn.execute(insert(table).values(label="inner"))
                sp.rollback()

            rows = conn.execute(select(table.c.label).order_by(table.c.id)).all()
            assert [r.label for r in rows] == ["outer"]

    def test_release_savepoint_keeps_changes(self, savepoint_table: tuple[Engine, Table]) -> None:
        engine, table = savepoint_table
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(insert(table).values(label="outer"))
                sp = conn.begin_nested()
                conn.execute(insert(table).values(label="inner"))
                sp.commit()  # RELEASE SAVEPOINT — keep inner

            rows = conn.execute(select(table.c.label).order_by(table.c.id)).all()
            assert sorted(r.label for r in rows) == ["inner", "outer"]

    def test_nested_savepoint_inside_savepoint(self, savepoint_table: tuple[Engine, Table]) -> None:
        engine, table = savepoint_table
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(insert(table).values(label="outer"))
                sp1 = conn.begin_nested()
                conn.execute(insert(table).values(label="sp1"))
                sp2 = conn.begin_nested()
                conn.execute(insert(table).values(label="sp2"))
                sp2.rollback()
                # sp1 still active and contains outer + sp1 rows
                sp1.commit()

            rows = conn.execute(select(table.c.label).order_by(table.c.id)).all()
            assert sorted(r.label for r in rows) == ["outer", "sp1"]


@pytest.mark.integration
class TestAsyncSavepointBasic:
    async def test_rollback_savepoint_preserves_outer_tx(
        self, async_savepoint_table: tuple[AsyncEngine, Table]
    ) -> None:
        engine, table = async_savepoint_table
        async with engine.connect() as conn:
            async with conn.begin():
                await conn.execute(insert(table).values(label="outer"))
                sp = await conn.begin_nested()
                await conn.execute(insert(table).values(label="inner"))
                await sp.rollback()

            result = await conn.execute(select(table.c.label).order_by(table.c.id))
            rows = result.all()
            assert [r.label for r in rows] == ["outer"]

    async def test_release_savepoint_keeps_changes(
        self, async_savepoint_table: tuple[AsyncEngine, Table]
    ) -> None:
        engine, table = async_savepoint_table
        async with engine.connect() as conn:
            async with conn.begin():
                await conn.execute(insert(table).values(label="outer"))
                sp = await conn.begin_nested()
                await conn.execute(insert(table).values(label="inner"))
                await sp.commit()

            result = await conn.execute(select(table.c.label).order_by(table.c.id))
            rows = result.all()
            assert sorted(r.label for r in rows) == ["inner", "outer"]

    async def test_nested_savepoint_inside_savepoint(
        self, async_savepoint_table: tuple[AsyncEngine, Table]
    ) -> None:
        engine, table = async_savepoint_table
        async with engine.connect() as conn:
            async with conn.begin():
                await conn.execute(insert(table).values(label="outer"))
                sp1 = await conn.begin_nested()
                await conn.execute(insert(table).values(label="sp1"))
                sp2 = await conn.begin_nested()
                await conn.execute(insert(table).values(label="sp2"))
                await sp2.rollback()
                await sp1.commit()

            result = await conn.execute(select(table.c.label).order_by(table.c.id))
            rows = result.all()
            assert sorted(r.label for r in rows) == ["outer", "sp1"]
