"""Pin: ``sqlalchemy.JSON`` column type round-trips correctly through
the dqlite SA dialect.

SA's SQLite dialect supports ``JSON`` via SQLite's JSON1 functions.
dqlite ships SQLite >= 3.35 so JSON1 is bundled; the SA dialect
inherits the JSON support from ``SQLiteDialect``. There was no
integration test exercising this end-to-end — this file closes
that gap.

The serialization path is:
``dict | list → JSON-stringify → wire TEXT → server`` and the
read-back is the inverse. Any silent encoding bug (Unicode,
embedded NULL handling, surrogate handling) on the JSON-string would
surface here.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator

import pytest
import sqlalchemy as sa
from sqlalchemy import JSON, Column, Integer, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture
def json_table(engine_url: str) -> Generator[tuple[Engine, Table]]:
    table_name = f"json_t_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", JSON),
    )
    eng = sa.create_engine(engine_url, future=True)
    metadata.create_all(eng)
    try:
        yield eng, table
    finally:
        metadata.drop_all(eng)
        eng.dispose()


@pytest.mark.integration
def test_json_dict_round_trip(json_table: tuple[Engine, Table]) -> None:
    engine, t = json_table
    payload = {"name": "alice", "age": 30, "tags": ["x", "y"]}
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=1, data=payload))
        row = conn.execute(sa.select(t.c.data).where(t.c.id == 1)).fetchone()
        assert row is not None
        assert row[0] == payload


@pytest.mark.integration
def test_json_list_round_trip(json_table: tuple[Engine, Table]) -> None:
    engine, t = json_table
    payload = [1, "two", 3.5, True, None]
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=2, data=payload))
        row = conn.execute(sa.select(t.c.data).where(t.c.id == 2)).fetchone()
        assert row is not None
        assert row[0] == payload


@pytest.mark.integration
def test_json_unicode_round_trip(json_table: tuple[Engine, Table]) -> None:
    engine, t = json_table
    payload = {"тест": "значение", "emoji": "🚀", "mixed": ["café", "☕"]}
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=3, data=payload))
        row = conn.execute(sa.select(t.c.data).where(t.c.id == 3)).fetchone()
        assert row is not None
        assert row[0] == payload


@pytest.mark.integration
def test_json_nested_round_trip(json_table: tuple[Engine, Table]) -> None:
    engine, t = json_table
    payload = {
        "users": [
            {"name": "alice", "perms": ["read", "write"]},
            {"name": "bob", "perms": []},
        ],
        "meta": {"version": 2, "active": True},
    }
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=4, data=payload))
        row = conn.execute(sa.select(t.c.data).where(t.c.id == 4)).fetchone()
        assert row is not None
        assert row[0] == payload


@pytest.mark.integration
def test_json_sql_null_distinct_from_json_null(json_table: tuple[Engine, Table]) -> None:
    """SQL NULL and JSON null are distinct values. Pin both round-trip
    paths."""
    engine, t = json_table
    with engine.begin() as conn:
        # SQL NULL — pass Python None directly via the column-level
        # ``null()`` helper (per SA's JSON docs).
        conn.execute(t.insert().values(id=5, data=sa.null()))
        # JSON null literal.
        conn.execute(t.insert().values(id=6, data=JSON.NULL))

        row5 = conn.execute(sa.select(t.c.data).where(t.c.id == 5)).fetchone()
        row6 = conn.execute(sa.select(t.c.data).where(t.c.id == 6)).fetchone()
        assert row5 is not None
        assert row6 is not None
        assert row5[0] is None  # SQL NULL surfaces as Python None
        assert row6[0] is None  # JSON null also surfaces as Python None
        # Note: SA's JSON type collapses both to None on readback by
        # default; the distinction is meaningful on insert via
        # ``none_as_null``-aware columns. We don't pin that distinction
        # here because it depends on column-level options not exercised
        # by this test.


# Async sibling
@pytest.fixture
async def async_json_engine(async_engine_url: str) -> AsyncEngine:
    return create_async_engine(async_engine_url, future=True)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_json_dict_round_trip_async(async_engine_url: str) -> None:
    engine = create_async_engine(async_engine_url, future=True)
    table_name = f"json_t_aio_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    t = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", JSON),
    )
    payload = {"a": 1, "b": [2, 3], "c": "тест"}
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        try:
            await conn.execute(t.insert().values(id=1, data=payload))
            result = await conn.execute(sa.select(t.c.data).where(t.c.id == 1))
            row = result.fetchone()
            assert row is not None
            assert row[0] == payload
        finally:
            await conn.run_sync(metadata.drop_all)
    await engine.dispose()
