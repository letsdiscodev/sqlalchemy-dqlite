"""The dialect docstring documents the sqlite_* (not dqlite_*) DDL kwarg prefix,
and the compile-time behaviour matches."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.schema import CreateTable

from sqlalchemydqlite.base import DqliteDialect


def test_sqlite_with_rowid_kwarg_takes_effect() -> None:
    m = MetaData()
    t = Table(
        "t1",
        m,
        Column("id", Integer, primary_key=True),
        sqlite_with_rowid=False,
    )
    compiled = str(CreateTable(t).compile(dialect=DqliteDialect()))
    assert "WITHOUT ROWID" in compiled


def test_dqlite_with_rowid_kwarg_rejected_at_construction() -> None:
    """The dqlite_* DDL prefix is refused at construction with an ArgumentError."""
    from sqlalchemy.exc import ArgumentError

    m = MetaData()
    with pytest.raises(ArgumentError, match="sqlite_with_rowid"):
        Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            dqlite_with_rowid=False,
        )
