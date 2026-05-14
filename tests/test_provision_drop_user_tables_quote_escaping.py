"""Pin: ``_drop_user_tables`` quotes embedded ``"`` in identifiers.

SQLite's identifier syntax requires that a delimited identifier
escape an embedded ``"`` as ``""``. A table named ``foo"bar``
(legal in SQLite because identifiers can carry arbitrary text when
double-quoted) would otherwise render as
``DROP TABLE IF EXISTS "foo"bar"`` — a syntax error.

The per-drop ``except Exception`` swallow inside
``_drop_user_tables`` would then mask the failure and the schema-
reset sweep promised by the docstring ("Best-effort drop of every
user-visible table") would silently half-complete.

Pin: the DDL emitted for a quote-bearing identifier must use the
doubled-quote escape so SQLite parses the full table name.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import sqlalchemydqlite.provision as provision


def _make_engine_with_tables(table_names: list[str]) -> MagicMock:
    select_result = MagicMock()
    select_result.fetchall.return_value = [(n,) for n in table_names]

    conn = MagicMock()
    sql_calls: list[str] = []

    def _exec(sql: str) -> Any:
        sql_calls.append(sql)
        if sql.startswith("SELECT"):
            return select_result
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
    """A table named ``foo"bar`` must be dropped as
    ``DROP TABLE IF EXISTS "foo""bar"`` — doubled quote per SQLite's
    delimited-identifier syntax.
    """
    eng = _make_engine_with_tables(['foo"bar'])
    provision._drop_user_tables(eng)

    drops = [s for s in eng._conn._sql_calls if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "foo""bar"']


def test_drop_escapes_multiple_embedded_double_quotes() -> None:
    """Doubling must apply to every ``"`` in the identifier, not just
    the first occurrence.
    """
    eng = _make_engine_with_tables(['a"b"c'])
    provision._drop_user_tables(eng)

    drops = [s for s in eng._conn._sql_calls if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "a""b""c"']


def test_drop_leaves_quote_free_identifier_unchanged() -> None:
    """The quote-doubling step must be a no-op for the common case
    of an identifier with no embedded ``"``.
    """
    eng = _make_engine_with_tables(["plain"])
    provision._drop_user_tables(eng)

    drops = [s for s in eng._conn._sql_calls if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "plain"']
