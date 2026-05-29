"""Pin: ``_drop_user_tables`` + ``_dqlite_run_reap_dbs`` invariants — dqlite
has no ``DROP DATABASE``, so these hooks drop every user-visible object."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
import sqlalchemy
from sqlalchemy.engine import url as sa_url

import sqlalchemydqlite.provision as provision


def _make_engine_with_objects(
    objects_by_type: dict[str, list[str]],
) -> MagicMock:
    """Mock engine whose ``exec_driver_sql(SELECT...)`` returns rows matching
    the requested type (from the second positional arg)."""

    conn = MagicMock()
    sql_calls: list[tuple[str, Any]] = []

    def _exec(sql: str, *args: Any) -> Any:
        sql_calls.append((sql, args))
        if sql.startswith("SELECT name") and args:
            (params,) = args
            obj_type = params[0]
            result = MagicMock()
            result.fetchall.return_value = [(n,) for n in objects_by_type.get(obj_type, [])]
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


def test_drops_each_listed_user_table() -> None:
    eng = _make_engine_with_objects({"table": ["foo", "bar"]})
    provision._drop_user_tables(eng)

    sqls = [call[0] for call in eng._conn._sql_calls]
    drops = [s for s in sqls if s.startswith("DROP")]
    assert 'DROP TABLE IF EXISTS "foo"' in drops
    assert 'DROP TABLE IF EXISTS "bar"' in drops
    # FK pragma flip wraps the loop.
    assert any("PRAGMA foreign_keys = OFF" in s for s in sqls)
    assert any("PRAGMA foreign_keys = ON" in s for s in sqls)


def test_select_uses_underscore_anchored_sqlite_filter() -> None:
    """Pin per-type SELECT: ``type=?`` and ``NOT LIKE 'sqlite_%'`` — the
    underscore anchor avoids sparing user tables named ``sqlitefoo``."""
    eng = _make_engine_with_objects({})
    provision._drop_user_tables(eng)
    selects = [call for call in eng._conn._sql_calls if call[0].startswith("SELECT name")]
    assert selects, "expected per-type SELECT(s)"
    for sql, _args in selects:
        assert "type=?" in sql
        assert "NOT LIKE 'sqlite_%'" in sql
    queried_types = [args[0][0] for _sql, args in selects]
    assert queried_types == ["trigger", "view", "index", "table"]


def test_per_drop_failure_does_not_abort_loop() -> None:
    """A DROP that raises is debug-logged; subsequent drops still run and the
    per-drop commit/rollback cycle keeps connection state clean."""

    conn = MagicMock()
    sql_calls: list[str] = []
    rollback_count = [0]

    def _exec(sql: str, *args: Any) -> Any:
        sql_calls.append(sql)
        if sql.startswith("SELECT name") and args:
            (params,) = args
            obj_type = params[0]
            result = MagicMock()
            if obj_type == "table":
                result.fetchall.return_value = [("first",), ("second",)]
            else:
                result.fetchall.return_value = []
            return result
        if sql.startswith("SELECT count"):
            result = MagicMock()
            result.scalar.return_value = 0
            return result
        if sql.startswith("DROP TABLE") and "first" in sql:
            raise RuntimeError("first drop failed")
        return MagicMock()

    def _rollback() -> None:
        rollback_count[0] += 1

    conn.exec_driver_sql.side_effect = _exec
    conn.rollback.side_effect = _rollback
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None
    eng = MagicMock()
    eng.connect.return_value = cm

    provision._drop_user_tables(eng)

    drops = [s for s in sql_calls if s.startswith("DROP TABLE")]
    assert drops == [
        'DROP TABLE IF EXISTS "first"',
        'DROP TABLE IF EXISTS "second"',
    ]
    # Failed first drop triggered exactly one rollback to clear autobegin.
    assert rollback_count[0] == 1


def test_connect_failure_swallowed_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``eng.connect`` failure logs at DEBUG and does not propagate."""
    eng = MagicMock()
    eng.connect.side_effect = RuntimeError("connect failed")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "during connect/exec" in msgs


def test_run_reap_dbs_forces_sync_drivername_when_input_is_aio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``dqlite+aio://`` input is rewritten to bare ``dqlite://`` before
    ``create_engine``; else SA routes to the async dialect and crashes."""
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
    assert captured_urls[0].drivername == "dqlite"


def test_run_reap_dbs_disposes_engine_when_drop_user_tables_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Engine ``dispose()`` runs in ``finally`` so a failing drop doesn't leak it."""
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
    """A per-ident failure lands in DEBUG; subsequent idents still processed."""
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


def test_drops_triggers_views_indexes_and_tables_in_dependency_order() -> None:
    """Queried/dropped in trigger -> view -> index -> table order so
    referential dependencies unwind cleanly even with FK enforcement off."""
    eng = _make_engine_with_objects(
        {
            "trigger": ["trg1"],
            "view": ["v_summary"],
            "index": ["ix_x"],
            "table": ["t_data"],
        }
    )
    provision._drop_user_tables(eng)

    drops = [call[0] for call in eng._conn._sql_calls if call[0].startswith("DROP")]
    assert 'DROP TRIGGER IF EXISTS "trg1"' in drops
    assert 'DROP VIEW IF EXISTS "v_summary"' in drops
    assert 'DROP INDEX IF EXISTS "ix_x"' in drops
    assert 'DROP TABLE IF EXISTS "t_data"' in drops
    trigger_pos = drops.index('DROP TRIGGER IF EXISTS "trg1"')
    view_pos = drops.index('DROP VIEW IF EXISTS "v_summary"')
    index_pos = drops.index('DROP INDEX IF EXISTS "ix_x"')
    table_pos = drops.index('DROP TABLE IF EXISTS "t_data"')
    assert trigger_pos < view_pos < index_pos < table_pos


def test_survivor_probe_emits_warning_when_objects_leak(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-empty survivor count fires a WARNING so operators spot a cleanup gap."""
    conn = MagicMock()

    def _exec(sql: str, *args: Any) -> Any:
        if sql.startswith("SELECT name"):
            result = MagicMock()
            result.fetchall.return_value = []
            return result
        if sql.startswith("SELECT count"):
            result = MagicMock()
            result.scalar.return_value = 3
            return result
        return MagicMock()

    conn.exec_driver_sql.side_effect = _exec
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None
    eng = MagicMock()
    eng.connect.return_value = cm

    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected WARNING on cleanup-incomplete survivor count"
    assert "3" in warnings[0].getMessage()
