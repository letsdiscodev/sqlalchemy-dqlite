"""Pin: ``DqliteDialect`` inherits from ``SQLiteDialect_pysqlite``,
not the abstract ``SQLiteDialect``.

Every concrete SA SQLite dialect (pysqlite, aiosqlite, pysqlcipher)
sits on ``SQLiteDialect_pysqlite``. Inheriting from the abstract
parent leaves us re-implementing pysqlite's defaults by hand and
opens a drift surface: any future addition to
``SQLiteDialect_pysqlite`` (new ``_isolation_lookup`` entries, new
``colspecs``, additional feature flags) does not propagate.

This test pins the inheritance so a future refactor cannot silently
move us back to the abstract base.
"""

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
    """The async dialect transitively inherits from
    ``SQLiteDialect_pysqlite`` via ``DqliteDialect``."""
    from sqlalchemydqlite.aio import DqliteDialect_aio

    assert issubclass(DqliteDialect_aio, SQLiteDialect_pysqlite)
    assert issubclass(DqliteDialect_aio, DqliteDialect)
