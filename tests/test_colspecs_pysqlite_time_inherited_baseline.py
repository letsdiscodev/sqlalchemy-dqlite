"""Pin: ``SQLiteDialect_pysqlite.colspecs[sqltypes.Time]`` is the
SQLite ``TIME`` class, NOT absent.

The dialect's colspecs-block comment previously claimed "pysqlite
has no Time colspec at all", motivating the ``sqltypes.Time:
_DqliteTime`` override. The claim was wrong: pysqlite inherits
``sqltypes.Time: TIME`` from ``SQLiteDialect.colspecs`` via
``util.update_copy`` (pysqlite does not remove the entry and does
not override). The correct rationale for the dqlite override is
that the inherited SQLite ``TIME`` calls ``processors.str_to_time``
on the raw cell and would raise ``TypeError`` on the already-
decoded ``datetime.time`` instances dqlitedbapi returns.

This pin defends the corrected comment from drifting back to the
old incorrect claim.
"""

from __future__ import annotations

from sqlalchemy.dialects.sqlite import base as sqlite_base
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.sql import sqltypes


def test_pysqlite_colspecs_inherits_sqlite_time_colspec() -> None:
    assert sqltypes.Time in SQLiteDialect_pysqlite.colspecs, (
        "pysqlite inherits sqltypes.Time: TIME from SQLiteDialect.colspecs"
    )
    assert SQLiteDialect_pysqlite.colspecs[sqltypes.Time] is sqlite_base.TIME
