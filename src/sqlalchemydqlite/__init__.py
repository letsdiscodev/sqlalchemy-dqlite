"""SQLAlchemy 2.0 dialect for dqlite."""

from typing import Final

from sqlalchemy.dialects.sqlite import (
    BLOB,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    JSON,
    NUMERIC,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    VARCHAR,
    Insert,
    insert,
)

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteCompiler, DqliteDialect

__version__: Final[str] = "0.1.3"

# SA convention (every shipped dialect package — sqlite, mysql, mssql,
# oracle, postgresql): expose ``<package>.dialect`` pointing at the
# default sync class. Pysqlite at ``.venv/.../sqlite/__init__.py:34``
# does ``base.dialect = dialect = pysqlite.dialect``; we follow the same
# pattern so ``from sqlalchemydqlite import dialect`` resolves
# canonically.
dialect = DqliteDialect
dialect_aio = DqliteDialect_aio

__all__ = [  # noqa: RUF022 - grouped: dialect entry points, then SA-shared SQLite types
    # Dialect entry points
    "DqliteCompiler",
    "DqliteDialect",
    "DqliteDialect_aio",
    "Insert",
    "__version__",
    "dialect",
    "dialect_aio",
    "insert",
    # SA-shared SQLite type re-exports (alphabetical within group)
    "BLOB",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "FLOAT",
    "INTEGER",
    "JSON",
    "NUMERIC",
    "REAL",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
]
