"""Pin: sa.func.floor(col) raises NotSupportedError at compile time.

dqlite has no UDF primitive (unlike pysqlite, which registers floor to cover SQLite
built without SQLITE_ENABLE_MATH_FUNCTIONS), so a runtime call would fail deep in the
wire layer with "no such function: floor". A math-enabled build can subclass
DqliteCompiler to relax the rule."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, func, select

from dqlitedbapi.exceptions import NotSupportedError
from sqlalchemydqlite.base import DqliteDialect


def test_sa_func_floor_raises_not_supported_at_compile_time() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.floor(t.c.x)).compile(dialect=DqliteDialect()))


def test_sa_func_floor_diagnostic_names_the_workaround() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    try:
        str(select(func.floor(t.c.x)).compile(dialect=DqliteDialect()))
    except NotSupportedError as exc:
        msg = str(exc)
        assert "CAST" in msg or "client-side" in msg
        assert "DqliteCompiler" not in msg or "subclass" in msg


def test_other_funcs_still_compile() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    sql = str(select(func.count(t.c.x)).compile(dialect=DqliteDialect()))
    assert "count" in sql.lower()


def test_floor_case_insensitive_name_matching() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    for name in ("floor", "FLOOR", "Floor"):
        with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
            str(select(getattr(func, name)(t.c.x)).compile(dialect=DqliteDialect()))


def test_floor_inside_compound_expression_still_trips_gate() -> None:
    """A nested floor (inside coalesce/sum/etc.) must still trip the gate."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.coalesce(func.floor(t.c.x), 0)).compile(dialect=DqliteDialect()))


def test_floor_in_where_clause_trips_gate() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    stmt = select(t.c.x).where(func.floor(t.c.x) > 0)
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(stmt.compile(dialect=DqliteDialect()))


def test_floor_in_order_by_trips_gate() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    stmt = select(t.c.x).order_by(func.floor(t.c.x))
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(stmt.compile(dialect=DqliteDialect()))


def test_floor_zero_args_still_trips_gate_before_arg_validation() -> None:
    """The gate fires on the name alone, before SA's arg-resolver masks it with a
    generic "wrong number of args" error."""
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.floor()).compile(dialect=DqliteDialect()))
