"""Pin ORM-level Session/AsyncSession.begin_nested() end-to-end against dqlite;
the ORM path threads through SA machinery the Connection-level pin doesn't cover.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Generator

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session


@pytest.fixture
def orm_savepoint_table(engine_url: str) -> Generator[tuple[Engine, Table]]:
    table_name = f"sp_orm_{uuid.uuid4().hex[:8]}"
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
async def async_orm_savepoint_table(
    async_engine_url: str,
) -> AsyncGenerator[tuple[AsyncEngine, Table]]:
    table_name = f"sp_orm_async_{uuid.uuid4().hex[:8]}"
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


def _row_ids(session: Session, table: Table) -> list[int]:
    return [row.id for row in session.execute(select(table.c.id).order_by(table.c.id))]


async def _async_row_ids(session: AsyncSession, table: Table) -> list[int]:
    result = await session.execute(select(table.c.id).order_by(table.c.id))
    return [row.id for row in result]


def test_sync_session_begin_nested_rollback_preserves_outer(
    orm_savepoint_table: tuple[Engine, Table],
) -> None:
    eng, table = orm_savepoint_table
    with Session(eng) as session, session.begin():
        session.execute(table.insert().values(id=1, label="outer"))
        try:
            with session.begin_nested():
                session.execute(table.insert().values(id=2, label="nested"))
                raise RuntimeError("rollback this savepoint")
        except RuntimeError:
            pass
        assert _row_ids(session, table) == [1]
    with Session(eng) as session:
        assert _row_ids(session, table) == [1]


def test_sync_session_begin_nested_release_keeps_changes(
    orm_savepoint_table: tuple[Engine, Table],
) -> None:
    eng, table = orm_savepoint_table
    with Session(eng) as session, session.begin():
        session.execute(table.insert().values(id=1, label="outer"))
        with session.begin_nested():
            session.execute(table.insert().values(id=2, label="nested"))
        assert _row_ids(session, table) == [1, 2]
    with Session(eng) as session:
        assert _row_ids(session, table) == [1, 2]


def test_sync_session_nested_inside_nested(
    orm_savepoint_table: tuple[Engine, Table],
) -> None:
    eng, table = orm_savepoint_table
    with Session(eng) as session, session.begin():
        session.execute(table.insert().values(id=1, label="outer"))
        with session.begin_nested():
            session.execute(table.insert().values(id=2, label="nested-1"))
            try:
                with session.begin_nested():
                    session.execute(table.insert().values(id=3, label="nested-2"))
                    raise RuntimeError("rollback inner savepoint only")
            except RuntimeError:
                pass
            assert _row_ids(session, table) == [1, 2]
        assert _row_ids(session, table) == [1, 2]
    with Session(eng) as session:
        assert _row_ids(session, table) == [1, 2]


async def test_async_session_begin_nested_rollback_preserves_outer(
    async_orm_savepoint_table: tuple[AsyncEngine, Table],
) -> None:
    eng, table = async_orm_savepoint_table
    async with AsyncSession(eng) as session, session.begin():
        await session.execute(table.insert().values(id=1, label="outer"))
        try:
            async with session.begin_nested():
                await session.execute(table.insert().values(id=2, label="nested"))
                raise RuntimeError("rollback this savepoint")
        except RuntimeError:
            pass
        assert await _async_row_ids(session, table) == [1]
    async with AsyncSession(eng) as session:
        assert await _async_row_ids(session, table) == [1]


async def test_async_session_begin_nested_release_keeps_changes(
    async_orm_savepoint_table: tuple[AsyncEngine, Table],
) -> None:
    eng, table = async_orm_savepoint_table
    async with AsyncSession(eng) as session, session.begin():
        await session.execute(table.insert().values(id=1, label="outer"))
        async with session.begin_nested():
            await session.execute(table.insert().values(id=2, label="nested"))
        assert await _async_row_ids(session, table) == [1, 2]
    async with AsyncSession(eng) as session:
        assert await _async_row_ids(session, table) == [1, 2]


async def test_async_session_nested_inside_nested(
    async_orm_savepoint_table: tuple[AsyncEngine, Table],
) -> None:
    eng, table = async_orm_savepoint_table
    async with AsyncSession(eng) as session, session.begin():
        await session.execute(table.insert().values(id=1, label="outer"))
        async with session.begin_nested():
            await session.execute(table.insert().values(id=2, label="nested-1"))
            try:
                async with session.begin_nested():
                    await session.execute(table.insert().values(id=3, label="nested-2"))
                    raise RuntimeError("rollback inner savepoint only")
            except RuntimeError:
                pass
            assert await _async_row_ids(session, table) == [1, 2]
        assert await _async_row_ids(session, table) == [1, 2]
    async with AsyncSession(eng) as session:
        assert await _async_row_ids(session, table) == [1, 2]
