"""``SQLiteDialect_pysqlite.colspecs[sqltypes.Time]`` is SQLite ``TIME``,
inherited (not absent). The dqlite override exists because that inherited
``TIME`` calls ``str_to_time`` and would raise on the already-decoded
``datetime.time`` dqlitedbapi returns."""

from __future__ import annotations

from sqlalchemy.dialects.sqlite import base as sqlite_base
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.sql import sqltypes


def test_pysqlite_colspecs_inherits_sqlite_time_colspec() -> None:
    assert sqltypes.Time in SQLiteDialect_pysqlite.colspecs, (
        "pysqlite inherits sqltypes.Time: TIME from SQLiteDialect.colspecs"
    )
    assert SQLiteDialect_pysqlite.colspecs[sqltypes.Time] is sqlite_base.TIME
