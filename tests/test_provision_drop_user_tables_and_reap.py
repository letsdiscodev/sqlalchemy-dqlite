"""Pin: ``_drop_user_tables`` + ``_dqlite_run_reap_dbs`` invariants.

dqlite has no ``DROP DATABASE`` primitive, so the provisioning hooks
implement a "drop every user table" workaround. Several load-bearing
invariants were unpinned:

1. The ``sqlite_master`` WHERE clause uses ``NOT LIKE 'sqlite_%'`` —
   underscore-anchored, NOT ``sqlite%`` which would also spare
   user-named ``sqlitefoo``.
2. Per-drop failures are swallowed inside the loop so a single
   failing DROP does not abort the entire reap.
3. ``connect`` failures are debug-logged and do NOT propagate.
4. ``run_reap_dbs`` force-rewrites a ``dqlite+aio://`` input to the
   bare sync drivername; otherwise ``create_engine`` would route to
   the async dialect that needs ``create_async_engine``.
5. ``run_reap_dbs`` disposes the engine in ``finally`` so a
   ``_drop_user_tables`` failure does not leak the engine.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
import sqlalchemy
from sqlalchemy.engine import url as sa_url

import sqlalchemydqlite.provision as provision


def _make_engine_with_tables(table_names: list[str]) -> MagicMock:
    """Build a mock engine whose ``connect()`` context manager yields a
    connection where ``exec_driver_sql(SELECT...).fetchall()`` returns
    a row per table name.
    """

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


def test_drops_each_listed_user_table() -> None:
    eng = _make_engine_with_tables(["foo", "bar"])
    provision._drop_user_tables(eng)

    sqls = eng._conn._sql_calls
    select = [s for s in sqls if s.startswith("SELECT")]
    drops = [s for s in sqls if s.startswith("DROP TABLE")]
    assert len(select) == 1
    assert drops == ['DROP TABLE IF EXISTS "foo"', 'DROP TABLE IF EXISTS "bar"']
    eng._conn.commit.assert_called_once_with()


def test_select_uses_underscore_anchored_sqlite_filter() -> None:
    """Pin the exact WHERE clause: ``NOT LIKE 'sqlite_%'``. Without
    the underscore, the filter would spare user tables named
    ``sqlitefoo`` AND drop internal ``sqlite_master``-class tables.
    """
    eng = _make_engine_with_tables([])
    provision._drop_user_tables(eng)
    sql = eng._conn._sql_calls[0]
    assert "FROM sqlite_master" in sql
    assert "type='table'" in sql
    assert "NOT LIKE 'sqlite_%'" in sql


def test_per_drop_failure_does_not_abort_loop() -> None:
    """A DROP that raises is debug-logged; subsequent drops still
    run and the outer ``commit`` still fires.
    """

    select_result = MagicMock()
    select_result.fetchall.return_value = [("first",), ("second",)]

    drop_calls: list[str] = []

    def _exec(sql: str) -> Any:
        if sql.startswith("SELECT"):
            return select_result
        drop_calls.append(sql)
        if "first" in sql:
            raise RuntimeError("first drop failed")
        return MagicMock()

    conn = MagicMock()
    conn.exec_driver_sql.side_effect = _exec
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None
    eng = MagicMock()
    eng.connect.return_value = cm

    provision._drop_user_tables(eng)

    assert drop_calls == [
        'DROP TABLE IF EXISTS "first"',
        'DROP TABLE IF EXISTS "second"',
    ]
    conn.commit.assert_called_once_with()


def test_connect_failure_swallowed_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If ``eng.connect`` raises, the helper logs at DEBUG and
    returns; the exception does NOT propagate.
    """
    eng = MagicMock()
    eng.connect.side_effect = RuntimeError("connect failed")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "during connect/exec" in msgs


def test_run_reap_dbs_forces_sync_drivername_when_input_is_aio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``dqlite+aio://...`` input must be rewritten to the bare
    ``dqlite://`` drivername before ``create_engine`` is called;
    otherwise SA would route to the async dialect that requires
    ``create_async_engine`` and crash.
    """
    captured_urls: list[sa_url.URL] = []

    def _fake_create_engine(rewritten_url: Any) -> Any:
        captured_urls.append(rewritten_url)
        eng = MagicMock()
        eng.dispose = MagicMock()
        return eng

    monkeypatch.setattr(sqlalchemy, "create_engine", _fake_create_engine)
    monkeypatch.setattr(provision, "_drop_user_tables", lambda _eng: None)

    provision._dqlite_run_reap_dbs("dqlite+aio://h:9001/db", ["w0"])

    assert captured_urls, "create_engine never called"
    # Force-sync rewrite: the drivername never carries ``+aio``.
    assert captured_urls[0].drivername == "dqlite"


def test_run_reap_dbs_disposes_engine_when_drop_user_tables_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Engine ``dispose()`` runs in ``finally`` so a failing
    ``_drop_user_tables`` does not leak the engine.
    """
    eng = MagicMock()
    eng.dispose = MagicMock()

    monkeypatch.setattr(sqlalchemy, "create_engine", lambda _u: eng)

    def _boom(_eng: Any) -> None:
        raise RuntimeError("partial failure")

    monkeypatch.setattr(provision, "_drop_user_tables", _boom)

    provision._dqlite_run_reap_dbs("dqlite:///db", ["w0"])

    eng.dispose.assert_called_once_with()


def test_run_reap_dbs_continues_to_next_ident_on_per_ident_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A per-ident failure (e.g. ``create_engine`` raising for one
    follower) lands in DEBUG; subsequent idents still get processed.
    """
    create_attempts: list[Any] = []

    def _fake_create_engine(rewritten_url: Any) -> Any:
        create_attempts.append(rewritten_url)
        if len(create_attempts) == 1:
            raise RuntimeError("create_engine failed for first ident")
        eng = MagicMock()
        eng.dispose = MagicMock()
        return eng

    monkeypatch.setattr(sqlalchemy, "create_engine", _fake_create_engine)
    monkeypatch.setattr(provision, "_drop_user_tables", lambda _eng: None)

    provision._dqlite_run_reap_dbs("dqlite:///db", ["w0", "w1"])

    assert len(create_attempts) == 2
