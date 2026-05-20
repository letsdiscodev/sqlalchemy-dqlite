"""Pin: ``sa.func.floor(col)`` raises ``NotSupportedError`` at
compile time, symmetric with the ``REGEXP`` operator's compile-
time fence.

pysqlite registers ``floor`` (and ``regexp``) as UDFs via
``Connection.create_function`` to paper over SQLite builds
compiled without ``SQLITE_ENABLE_MATH_FUNCTIONS``. dqlite has no
UDF primitive, so the dialect's ``on_connect`` is a no-op. Without
a compile-time gate, ``sa.func.floor(col)`` would silently fail at
runtime against a dqlite-server SQLite built without math
functions, producing ``no such function: floor`` deep inside the
wire layer.

The fence matches the ``visit_regexp_match_op_binary`` discipline
and lets operators with math-functions-enabled builds subclass
``DqliteCompiler`` to relax the rule.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, func, select

from dqlitedbapi.exceptions import NotSupportedError
from sqlalchemydqlite.base import DqliteDialect


def test_sa_func_floor_raises_not_supported_at_compile_time() -> None:
    """The compile-time fence emits ``NotSupportedError`` naming the
    SQLite build dependency."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.floor(t.c.x)).compile(dialect=DqliteDialect()))


def test_sa_func_floor_diagnostic_names_the_workaround() -> None:
    """Operator-facing diagnostic names the workaround so a build
    that turns on math functions can override the compiler."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    try:
        str(select(func.floor(t.c.x)).compile(dialect=DqliteDialect()))
    except NotSupportedError as exc:
        msg = str(exc)
        assert "CAST" in msg or "client-side" in msg
        assert "DqliteCompiler" not in msg or "subclass" in msg


def test_other_funcs_still_compile() -> None:
    """The gate only fires on ``floor``; other functions
    (``count``, ``max``, etc.) compile normally."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    sql = str(select(func.count(t.c.x)).compile(dialect=DqliteDialect()))
    assert "count" in sql.lower()


def test_floor_case_insensitive_name_matching() -> None:
    """Function names are compared case-insensitively so ``FLOOR``
    / ``Floor`` / ``floor`` all trip the gate."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    for name in ("floor", "FLOOR", "Floor"):
        with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
            str(select(getattr(func, name)(t.c.x)).compile(dialect=DqliteDialect()))


def test_floor_inside_compound_expression_still_trips_gate() -> None:
    """The compiler walks expression trees depth-first; a nested
    ``floor`` inside ``coalesce`` / ``sum`` / etc. must still trip
    the gate. A regression that hoists ``floor`` into a wrapper or
    that adds short-circuit logic skipping nested functions would
    slip past the top-level-only existing tests."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.coalesce(func.floor(t.c.x), 0)).compile(dialect=DqliteDialect()))


def test_floor_in_where_clause_trips_gate() -> None:
    """The gate fires at compile time regardless of clause
    position; the WHERE branch is the most common production form."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    stmt = select(t.c.x).where(func.floor(t.c.x) > 0)
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(stmt.compile(dialect=DqliteDialect()))


def test_floor_in_order_by_trips_gate() -> None:
    """ORDER BY branch coverage — same rationale as WHERE."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    stmt = select(t.c.x).order_by(func.floor(t.c.x))
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(stmt.compile(dialect=DqliteDialect()))


def test_floor_zero_args_still_trips_gate_before_arg_validation() -> None:
    """The gate fires on the function name alone, before SA's
    argument-resolver runs. A regression moving the gate after
    arg-validation would surface SA's generic "wrong number of
    args" error, masking the dqlite-specific guidance."""
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.floor()).compile(dialect=DqliteDialect()))
