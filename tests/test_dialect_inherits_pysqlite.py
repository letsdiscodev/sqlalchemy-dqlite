"""Pin: ``DqliteDialect`` inherits from ``SQLiteDialect_pysqlite``, not the abstract
``SQLiteDialect`` (whose defaults we'd otherwise re-implement by hand)."""

from __future__ import annotations

from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite

from sqlalchemydqlite import DqliteDialect


def test_sync_dialect_inherits_from_sqlite_dialect_pysqlite() -> None:
    assert issubclass(DqliteDialect, SQLiteDialect_pysqlite), (
        f"DqliteDialect must inherit from SQLiteDialect_pysqlite to receive "
        f"the canonical SA SQLite-dialect defaults; current MRO: "
        f"{[c.__name__ for c in DqliteDialect.__mro__]}"
    )


def test_async_dialect_inherits_from_sync_dqlite_dialect() -> None:
    from sqlalchemydqlite.aio import DqliteDialect_aio

    assert issubclass(DqliteDialect_aio, SQLiteDialect_pysqlite)
    assert issubclass(DqliteDialect_aio, DqliteDialect)
