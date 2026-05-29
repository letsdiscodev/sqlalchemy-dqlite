"""A dqlite_* DDL kwarg on Table/Index/Column/Constraint raises ArgumentError at
construction (a "did you mean sqlite_*" hint). SA keys dialect kwargs by the
written prefix, so dqlite_* would otherwise be silently dropped at compile time."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, Table
from sqlalchemy.exc import ArgumentError

# Import side effect: installs the SA event listener enforcing the prefix discipline.
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
    m = MetaData()
    t = Table(
        "t_ok",
        m,
        Column("id", Integer, primary_key=True),
        sqlite_with_rowid=False,
    )
    assert t.dialect_options["sqlite"]["with_rowid"] is False


def test_table_without_dialect_kwargs_works() -> None:
    m = MetaData()
    Table("t_plain", m, Column("id", Integer, primary_key=True))


def test_index_dqlite_where_raises_argument_error() -> None:
    m = MetaData()
    t = Table("t_idx", m, Column("id", Integer, primary_key=True), Column("v", Integer))
    with pytest.raises(ArgumentError, match="sqlite_where"):
        Index("ix_bad", t.c.v, dqlite_where="v > 0")
