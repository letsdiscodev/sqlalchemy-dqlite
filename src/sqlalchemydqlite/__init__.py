"""SQLAlchemy 2.0 dialect for dqlite."""

import logging
from typing import Final as _Final

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

__version__: _Final[str] = "0.4.0"

# SA convention: every shipped dialect package exposes ``<package>.dialect``
# pointing at the default sync class.
dialect: _Final[type[DqliteDialect]] = DqliteDialect
dialect_aio: _Final[type[DqliteDialect_aio]] = DqliteDialect_aio

__all__ = [
    "DqliteCompiler",
    "DqliteDialect",
    "DqliteDialect_aio",
    "Insert",
    "__version__",
    "dialect",
    "dialect_aio",
    "insert",
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

# NullHandler keeps the library quiet until the application configures logging.
logging.getLogger(__name__).addHandler(logging.NullHandler())
