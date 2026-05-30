"""Pin: ``do_executemany`` is defined locally for drift defence, so an
upstream SA refactor of ``DefaultDialect.do_executemany`` can't silently
change executemany semantics for this dialect.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


def test_do_executemany_body_is_byte_equivalent_to_sa_default() -> None:
    """Body must match SA's one-line pass-through."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()

    statement = "INSERT INTO t VALUES (?)"
    parameters = [(1,), (2,), (3,)]

    dialect.do_executemany(cursor, statement, parameters)

    cursor.executemany.assert_called_once_with(statement, parameters)


def test_do_executemany_accepts_optional_context_kwarg() -> None:
    """Signature accepts ``context`` so SA call sites passing it still work."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()
    statement = "UPDATE t SET v = ? WHERE k = ?"
    parameters = [(10, 1), (20, 2)]

    dialect.do_executemany(cursor, statement, parameters, context=MagicMock())

    cursor.executemany.assert_called_once_with(statement, parameters)
