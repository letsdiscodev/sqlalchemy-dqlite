"""Pin: SA constructs (``Table`` / ``Index`` / ``Column`` /
``Constraint``) that carry a ``dqlite_*`` DDL kwarg surface a
``ArgumentError`` at construction with a "did you mean ``sqlite_*``"
hint, mirroring the runtime fail-fast discipline that
``_validate_connect_kwargs`` provides for the connect-side.

SA's ``DialectKWArgs`` keys per-construct dialect kwargs by the
user-written prefix, so ``Table(..., dqlite_with_rowid=False)``
stored under ``dialect_options['dqlite']`` while the inherited
``SQLiteDDLCompiler`` read exclusively from
``dialect_options['sqlite']``. The dqlite-prefixed value was
silently dropped at compile time. The dialect docstring warned
about this in prose; the new listener enforces it at construction
time so a copy-paste mistake from a pysqlite-tagged-with-dialect-
name example surfaces a sharp error rather than a silently-dropped
kwarg.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, Table
from sqlalchemy.exc import ArgumentError

# Importing the dialect module installs the SA event listener that
# enforces the prefix discipline. The fail-fast behaviour depends on
# that import side effect.
import sqlalchemydqlite.base  # noqa: F401


def test_table_dqlite_with_rowid_raises_argument_error() -> None:
    m = MetaData()
    with pytest.raises(ArgumentError) as exc_info:
        Table(
            "t_bad",
            m,
            Column("id", Integer, primary_key=True),
            dqlite_with_rowid=False,
        )
    msg = str(exc_info.value)
    assert "sqlite_with_rowid" in msg, msg
    assert "dqlite_" in msg, msg


def test_table_sqlite_with_rowid_still_works() -> None:
    """Sanity: the correct prefix is unaffected."""
    m = MetaData()
    t = Table(
        "t_ok",
        m,
        Column("id", Integer, primary_key=True),
        sqlite_with_rowid=False,
    )
    assert t.dialect_options["sqlite"]["with_rowid"] is False


def test_table_without_dialect_kwargs_works() -> None:
    """Sanity: bare tables are unaffected."""
    m = MetaData()
    Table("t_plain", m, Column("id", Integer, primary_key=True))


def test_index_dqlite_where_raises_argument_error() -> None:
    m = MetaData()
    t = Table("t_idx", m, Column("id", Integer, primary_key=True), Column("v", Integer))
    with pytest.raises(ArgumentError, match="sqlite_where"):
        Index("ix_bad", t.c.v, dqlite_where="v > 0")
