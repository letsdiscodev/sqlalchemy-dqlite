"""Pin: ``DqliteDialect.do_executemany`` is defined locally for drift
defence.

The dialect already pins ``supports_sane_multi_rowcount = True`` and
``insert_executemany_returning = True`` locally; the matching
dispatch hook must be pinned too so an upstream SA refactor of
``DefaultDialect.do_executemany`` (per-parameter iteration,
paramstyle conversion, ...) cannot silently change executemany
semantics for this dialect.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


def test_do_executemany_is_overridden_locally() -> None:
    assert "do_executemany" in DqliteDialect.__dict__, (
        "DqliteDialect.do_executemany must be pinned locally for "
        "drift defence — see the override's docstring for rationale."
    )


def test_do_executemany_body_is_byte_equivalent_to_sa_default() -> None:
    """The body must be the same one-line pass-through SA's
    ``DefaultDialect.do_executemany`` is. Drive a stub cursor and
    assert it sees one call with the verbatim arguments."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()

    statement = "INSERT INTO t VALUES (?)"
    parameters = [(1,), (2,), (3,)]

    dialect.do_executemany(cursor, statement, parameters)

    cursor.executemany.assert_called_once_with(statement, parameters)


def test_do_executemany_accepts_optional_context_kwarg() -> None:
    """SA's default signature accepts ``context`` (sometimes the
    ``ExecutionContext`` instance). Mirror it so SA's call sites
    that pass ``context=...`` continue to work."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()
    statement = "UPDATE t SET v = ? WHERE k = ?"
    parameters = [(10, 1), (20, 2)]

    dialect.do_executemany(cursor, statement, parameters, context=MagicMock())

    cursor.executemany.assert_called_once_with(statement, parameters)
