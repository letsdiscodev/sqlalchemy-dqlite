"""SAVEPOINT names that require double-quoting must round-trip through
SA → dbapi → client → wire → server.

SQLAlchemy auto-generates bare identifiers (``sa_savepoint_N``); user
code rarely supplies SAVEPOINT names directly. But raw ``connection
.execute(text('SAVEPOINT "..."'))`` is legal SQLite and the dialect
must transparently pass quoted identifiers through.

The dialect inherits ``do_savepoint`` / ``do_release_savepoint`` /
``do_rollback_to_savepoint`` from ``DefaultDialect``; this test pins
the contract that quoted-identifier SAVEPOINTs work end-to-end. After
the client-side tracker fix that stops tracking quoted names locally
(``_parse_savepoint_name`` returns None for double-quoted forms), the
local ``in_transaction`` flag will reflect the trade-off — but the
SAVEPOINT round-trip itself remains functional.
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
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture
def quoted_sp_table(engine_url: str) -> Generator[tuple[Engine, Table]]:
    table_name = f"sp_quoted_{uuid.uuid4().hex[:8]}"
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
async def async_quoted_sp_table(
    async_engine_url: str,
) -> AsyncGenerator[tuple[AsyncEngine, Table]]:
    table_name = f"sp_quoted_async_{uuid.uuid4().hex[:8]}"
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
class TestSavepointQuotedIdentifiers:
    """Sync round-trip tests for SAVEPOINT names that require quoting.

    Use raw ``text()`` so the assertions are about end-to-end behaviour
    of the dialect / dbapi / wire stack, not about SA's
    ``IdentifierPreparer`` internals. The identifier formatter is SA's
    responsibility; we only need to know dqlite accepts quoted names.
    """

    def test_savepoint_with_reserved_word_name(self, quoted_sp_table: tuple[Engine, Table]) -> None:
        engine, table = quoted_sp_table
        with engine.connect() as conn, conn.begin():
            conn.execute(insert(table).values(label="outer"))
            conn.execute(text('SAVEPOINT "select"'))
            conn.execute(insert(table).values(label="inner"))
            conn.execute(text('RELEASE SAVEPOINT "select"'))
            rows = conn.execute(select(table.c.label).order_by(table.c.id)).all()
            assert sorted(r.label for r in rows) == ["inner", "outer"]

    def test_savepoint_with_space_in_name(self, quoted_sp_table: tuple[Engine, Table]) -> None:
        engine, table = quoted_sp_table
        with engine.connect() as conn, conn.begin():
            conn.execute(insert(table).values(label="outer"))
            conn.execute(text('SAVEPOINT "name with space"'))
            conn.execute(insert(table).values(label="inner"))
            conn.execute(text('RELEASE SAVEPOINT "name with space"'))
            rows = conn.execute(select(table.c.label).order_by(table.c.id)).all()
            assert sorted(r.label for r in rows) == ["inner", "outer"]

    def test_savepoint_with_leading_digit_name(self, quoted_sp_table: tuple[Engine, Table]) -> None:
        engine, table = quoted_sp_table
        with engine.connect() as conn, conn.begin():
            conn.execute(insert(table).values(label="outer"))
            conn.execute(text('SAVEPOINT "1stcheckpoint"'))
            conn.execute(insert(table).values(label="inner"))
            conn.execute(text('ROLLBACK TO SAVEPOINT "1stcheckpoint"'))
            conn.execute(text('RELEASE SAVEPOINT "1stcheckpoint"'))
            # Inner row was rolled back; outer survives.
            rows = conn.execute(select(table.c.label).order_by(table.c.id)).all()
            assert [r.label for r in rows] == ["outer"]


@pytest.mark.integration
class TestSavepointQuotedIdentifiersAsync:
    """Async sibling of the sync class."""

    async def test_savepoint_with_reserved_word_name(
        self, async_quoted_sp_table: tuple[AsyncEngine, Table]
    ) -> None:
        engine, table = async_quoted_sp_table
        async with engine.connect() as conn, conn.begin():
            await conn.execute(insert(table).values(label="outer"))
            await conn.execute(text('SAVEPOINT "select"'))
            await conn.execute(insert(table).values(label="inner"))
            await conn.execute(text('RELEASE SAVEPOINT "select"'))
            result = await conn.execute(select(table.c.label).order_by(table.c.id))
            rows = result.all()
            assert sorted(r.label for r in rows) == ["inner", "outer"]

    async def test_savepoint_with_space_in_name(
        self, async_quoted_sp_table: tuple[AsyncEngine, Table]
    ) -> None:
        engine, table = async_quoted_sp_table
        async with engine.connect() as conn, conn.begin():
            await conn.execute(insert(table).values(label="outer"))
            await conn.execute(text('SAVEPOINT "name with space"'))
            await conn.execute(insert(table).values(label="inner"))
            await conn.execute(text('RELEASE SAVEPOINT "name with space"'))
            result = await conn.execute(select(table.c.label).order_by(table.c.id))
            rows = result.all()
            assert sorted(r.label for r in rows) == ["inner", "outer"]
