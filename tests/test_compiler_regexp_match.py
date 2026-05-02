"""``regexp_match`` raises at compile time (no UDF on the wire).

dqlite has no UDF primitive, so the inherited SQLite ``REGEXP`` operator
cannot be honoured at execute. Raise at compile so the diagnostic
surfaces before round-tripping to the cluster.
"""

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, select

from sqlalchemydqlite import DqliteDialect
from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteCompiler


@pytest.fixture
def stmt_table() -> Table:
    md = MetaData()
    return Table(
        "t",
        md,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )


def test_regexp_match_raises_at_compile_with_dqlite_diagnostic(
    stmt_table: Table,
) -> None:
    """Positive form: ``col.regexp_match(pattern)``."""
    from dqlitedbapi.exceptions import NotSupportedError

    stmt = select(stmt_table).where(stmt_table.c.name.regexp_match("foo"))

    with pytest.raises(NotSupportedError) as excinfo:
        stmt.compile(dialect=DqliteDialect())

    msg = str(excinfo.value).lower()
    assert "dqlite" in msg
    assert "regexp" in msg or "udf" in msg


def test_not_regexp_match_raises_at_compile(stmt_table: Table) -> None:
    """Negated form ``~col.regexp_match(pattern)``: SA dispatches to
    ``visit_not_regexp_match_op_binary`` which is a separate visitor on
    the parent compiler — both must override.
    """
    from dqlitedbapi.exceptions import NotSupportedError

    stmt = select(stmt_table).where(~stmt_table.c.name.regexp_match("foo"))

    with pytest.raises(NotSupportedError):
        stmt.compile(dialect=DqliteDialect())


def test_async_dialect_inherits_dqlite_compiler() -> None:
    """``DqliteDialect_aio`` inherits from ``DqliteDialect`` so the
    statement-compiler binding propagates without a separate pin.
    """
    assert DqliteDialect_aio.statement_compiler is DqliteCompiler
    assert DqliteDialect.statement_compiler is DqliteCompiler


def test_dqlite_compiler_subclasses_sqlite_compiler() -> None:
    """``DqliteCompiler`` MUST be a ``SQLiteCompiler`` subclass so all
    other SQLite-specific compile rules (ON CONFLICT, RETURNING, etc.)
    are inherited unchanged.
    """
    from sqlalchemy.dialects.sqlite.base import SQLiteCompiler

    assert issubclass(DqliteCompiler, SQLiteCompiler)
