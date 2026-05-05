"""SQLAlchemy 2.0 dialect for dqlite."""

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

from sqlalchemydqlite.base import DqliteDialect

__version__ = "0.1.3"

# SA convention (every shipped dialect package — sqlite, mysql, mssql,
# oracle, postgresql): expose ``<package>.dialect`` pointing at the
# default sync class. Pysqlite at ``.venv/.../sqlite/__init__.py:34``
# does ``base.dialect = dialect = pysqlite.dialect``; we follow the same
# pattern so ``from sqlalchemydqlite import dialect`` resolves
# canonically.
dialect = DqliteDialect

__all__ = [  # noqa: RUF022 - grouped: dialect entry points, then SA-shared SQLite types
    # Dialect entry points
    "DqliteDialect",
    "Insert",
    "__version__",
    "dialect",
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
