"""Pin: ``_drop_user_tables`` doubles an embedded ``"`` in identifiers per
SQLite's delimited-identifier syntax — else ``foo"bar`` yields invalid DDL
that the per-drop swallow would silently mask."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import sqlalchemydqlite.provision as provision


def _make_engine_with_tables(table_names: list[str]) -> MagicMock:
    """Mock engine where only the ``type='table'`` SELECT returns rows."""

    conn = MagicMock()
    sql_calls: list[str] = []

    def _exec(sql: str, *args: Any) -> Any:
        sql_calls.append(sql)
        if sql.startswith("SELECT name") and args:
            (params,) = args
            result = MagicMock()
            if params[0] == "table":
                result.fetchall.return_value = [(n,) for n in table_names]
            else:
                result.fetchall.return_value = []
            return result
        if sql.startswith("SELECT count"):
            result = MagicMock()
            result.scalar.return_value = 0
            return result
        return MagicMock()

    conn.exec_driver_sql.side_effect = _exec
    conn._sql_calls = sql_calls

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None

    eng = MagicMock()
    eng.connect.return_value = cm
    eng._conn = conn
    return eng


def test_drop_escapes_embedded_double_quote_in_identifier() -> None:
    eng = _make_engine_with_tables(['foo"bar'])
    provision._drop_user_tables(eng)

    drops = [s for s in eng._conn._sql_calls if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "foo""bar"']


def test_drop_escapes_multiple_embedded_double_quotes() -> None:
    """Doubling applies to every ``"``, not just the first."""
    eng = _make_engine_with_tables(['a"b"c'])
    provision._drop_user_tables(eng)

    drops = [s for s in eng._conn._sql_calls if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "a""b""c"']


def test_drop_leaves_quote_free_identifier_unchanged() -> None:
    """Quote-doubling is a no-op for an identifier with no embedded ``"``."""
    eng = _make_engine_with_tables(["plain"])
    provision._drop_user_tables(eng)

    drops = [s for s in eng._conn._sql_calls if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "plain"']
