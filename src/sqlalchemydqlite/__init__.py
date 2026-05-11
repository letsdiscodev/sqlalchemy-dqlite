"""SQLAlchemy 2.0 dialect for dqlite."""

import logging
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

__version__: Final[str] = "0.1.5"

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

# Convention from the Python logging HOWTO: attach a ``NullHandler``
# to the library's top-level logger so applications that have not
# configured logging don't see the ``lastResort`` stderr emission,
# and downstream code can silence the library cleanly via
# ``getLogger("sqlalchemydqlite").propagate = False``.
logging.getLogger(__name__).addHandler(logging.NullHandler())
