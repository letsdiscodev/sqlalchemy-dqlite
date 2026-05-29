"""Pin SQLAlchemy ``INSERT ... ON CONFLICT`` (UPSERT) round-trips end-to-end
through the dqlite SA dialect.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine


@pytest.fixture
def upsert_table(engine_url: str) -> Generator[tuple[Engine, Table]]:
    table_name = f"upsert_t_{uuid.uuid4().hex[:8]}"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("v", String),
    )
    eng = sa.create_engine(engine_url, future=True)
    metadata.create_all(eng)
    try:
        yield eng, table
    finally:
        metadata.drop_all(eng)
        eng.dispose()


@pytest.mark.integration
def test_upsert_do_nothing_on_existing_row_leaves_data_unchanged(
    upsert_table: tuple[Engine, Table],
) -> None:
    engine, t = upsert_table
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=1, v="a"))
        stmt = sqlite_insert(t).values(id=1, v="b").on_conflict_do_nothing()
        conn.execute(stmt)

        row = conn.execute(sa.select(t.c.v).where(t.c.id == 1)).fetchone()
        assert row is not None
        assert row[0] == "a", "do_nothing must NOT replace existing row"


@pytest.mark.integration
def test_upsert_do_update_replaces_row(
    upsert_table: tuple[Engine, Table],
) -> None:
    engine, t = upsert_table
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=1, v="a"))
        stmt = sqlite_insert(t).values(id=1, v="b")
        stmt = stmt.on_conflict_do_update(index_elements=["id"], set_={"v": "b"})
        conn.execute(stmt)

        row = conn.execute(sa.select(t.c.v).where(t.c.id == 1)).fetchone()
        assert row is not None
        assert row[0] == "b"


@pytest.mark.integration
def test_upsert_on_non_existent_row_acts_as_plain_insert(
    upsert_table: tuple[Engine, Table],
) -> None:
    """UPSERT on a non-existent row inserts; the conflict branch is never reached."""
    engine, t = upsert_table
    with engine.begin() as conn:
        stmt = sqlite_insert(t).values(id=42, v="fresh").on_conflict_do_nothing()
        conn.execute(stmt)

        row = conn.execute(sa.select(t.c.v).where(t.c.id == 42)).fetchone()
        assert row is not None
        assert row[0] == "fresh"


@pytest.mark.integration
def test_upsert_excluded_alias_replaces_with_proposed_value(
    upsert_table: tuple[Engine, Table],
) -> None:
    """The ``excluded`` alias references the proposed row's values, letting
    do_update write them into the existing row without restating the literal."""
    engine, t = upsert_table
    with engine.begin() as conn:
        conn.execute(t.insert().values(id=1, v="original"))
        stmt = sqlite_insert(t).values(id=1, v="proposed")
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={"v": stmt.excluded.v},
        )
        conn.execute(stmt)

        row = conn.execute(sa.select(t.c.v).where(t.c.id == 1)).fetchone()
        assert row is not None
        assert row[0] == "proposed"
