"""Verify executemany RETURNING behaviour for INSERT / UPDATE / DELETE.

The SQLAlchemy dialect flags ``insert_executemany_returning``,
``update_executemany_returning`` and ``delete_executemany_returning``
describe whether the dialect can deliver per-parameter-set RETURNING
rows in a single round-trip. These tests exercise the wire path
end-to-end against the test cluster so the flags can be pinned
confidently on the dialect itself.
"""

import uuid
from collections.abc import Generator

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    bindparam,
    create_engine,
    delete,
    insert,
    update,
)
from sqlalchemy.engine import Engine


@pytest.fixture
def bulk_table(engine_url: str) -> Generator[tuple[Engine, Table]]:
    """Per-test engine + freshly-named table so parallel runs don't collide."""
    table_name = f"bulk_returning_{uuid.uuid4().hex[:8]}"
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


class TestInsertExecutemanyReturning:
    def test_multi_row_insert_returns_each_row(self, bulk_table: tuple[Engine, Table]) -> None:
        engine, table = bulk_table
        stmt = insert(table).returning(table.c.id, table.c.label)
        with engine.begin() as conn:
            result = conn.execute(
                stmt,
                [
                    {"id": 1, "label": "a"},
                    {"id": 2, "label": "b"},
                    {"id": 3, "label": "c"},
                ],
            )
            rows = result.all()
        assert sorted(rows) == [(1, "a"), (2, "b"), (3, "c")]


class TestUpdateExecutemanyReturning:
    def test_multi_row_update_returns_each_row(self, bulk_table: tuple[Engine, Table]) -> None:
        engine, table = bulk_table
        with engine.begin() as conn:
            conn.execute(
                insert(table),
                [
                    {"id": 10, "label": "old-a"},
                    {"id": 11, "label": "old-b"},
                ],
            )

        stmt = (
            update(table)
            .where(table.c.id == bindparam("target_id"))
            .values(label=bindparam("new_label"))
            .returning(table.c.id, table.c.label)
        )
        with engine.begin() as conn:
            result = conn.execute(
                stmt,
                [
                    {"target_id": 10, "new_label": "new-a"},
                    {"target_id": 11, "new_label": "new-b"},
                ],
            )
            rows = result.all()
        assert sorted(rows) == [(10, "new-a"), (11, "new-b")]


class TestDeleteExecutemanyReturning:
    def test_multi_row_delete_returns_each_row(self, bulk_table: tuple[Engine, Table]) -> None:
        engine, table = bulk_table
        with engine.begin() as conn:
            conn.execute(
                insert(table),
                [
                    {"id": 20, "label": "x"},
                    {"id": 21, "label": "y"},
                ],
            )

        stmt = (
            delete(table)
            .where(table.c.id == bindparam("target_id"))
            .returning(table.c.id, table.c.label)
        )
        with engine.begin() as conn:
            result = conn.execute(
                stmt,
                [{"target_id": 20}, {"target_id": 21}],
            )
            rows = result.all()
        assert sorted(rows) == [(20, "x"), (21, "y")]
