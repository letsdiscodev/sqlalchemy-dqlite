"""Pin: the dialect docstring documents the ``sqlite_*`` (not
``dqlite_*``) DDL kwarg-prefix asymmetry, and the behaviour matches.

``SA.DialectKWArgs`` keys per-construct dialect kwargs by the
user-written prefix, so ``Table(..., dqlite_with_rowid=False)``
stores under ``dialect_options['dqlite']``. The inherited
``SQLiteDDLCompiler``, however, reads exclusively from
``dialect_options['sqlite']``. The ``dqlite_*`` form is silently
dropped at compile time.

The fix is documentation only — overriding the DDL compiler to
read both prefixes would duplicate ~150 lines of SA code and risk
DDL drift against future SA SQLite releases. Pin both that the
docstring carries the warning AND that the actual compile-time
behaviour matches.
"""

from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.schema import CreateTable

from sqlalchemydqlite.base import DqliteDialect


def test_dialect_docstring_documents_sqlite_prefix_for_ddl_kwargs() -> None:
    """The class docstring must explicitly state that DDL kwargs use
    the ``sqlite_*`` prefix (not ``dqlite_*``)."""
    doc = DqliteDialect.__doc__ or ""
    assert "sqlite_*" in doc, "docstring must name the sqlite_* prefix"
    assert "NOT" in doc and "dqlite_*" in doc, (
        "docstring must explicitly warn against dqlite_* prefix"
    )


def test_sqlite_with_rowid_kwarg_takes_effect() -> None:
    """The ``sqlite_with_rowid=False`` form emits ``WITHOUT ROWID``."""
    m = MetaData()
    t = Table(
        "t1",
        m,
        Column("id", Integer, primary_key=True),
        sqlite_with_rowid=False,
    )
    compiled = str(CreateTable(t).compile(dialect=DqliteDialect()))
    assert "WITHOUT ROWID" in compiled


def test_dqlite_with_rowid_kwarg_silently_ignored() -> None:
    """Pin the documented contract — the naturally-spelled
    ``dqlite_*`` form is silently dropped at compile time. A
    regression that adds dqlite_* support without also adding a
    deprecation path would fail this pin."""
    m = MetaData()
    t = Table(
        "t2",
        m,
        Column("id", Integer, primary_key=True),
        dqlite_with_rowid=False,
    )
    compiled = str(CreateTable(t).compile(dialect=DqliteDialect()))
    assert "WITHOUT ROWID" not in compiled, (
        "Documented contract: dqlite_with_rowid is silently dropped. "
        "If this fails, the dialect grew dqlite_* support — update "
        "the docstring to match."
    )
