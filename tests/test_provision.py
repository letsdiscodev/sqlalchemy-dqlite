"""SA testing-provision hooks: driver URLs, ident sanitisation, drop/reap, log hygiene."""

from __future__ import annotations

import logging
import os
import re
from typing import Any
from unittest.mock import MagicMock

import pytest
import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.engine import url as sa_url

import sqlalchemydqlite.provision  # noqa: F401  side-effect registration
import sqlalchemydqlite.provision as provision
from sqlalchemydqlite import provision as p
from sqlalchemydqlite.provision import (
    _DRIVERNAMES,
    _LOG_TRUNCATE_MAX_CHARS,
    _dqlite_create_db,
    _dqlite_drop_db,
    _dqlite_generate_driver_url,
    _format_url,
    _safe_for_log,
)


def test_unknown_driver_returns_none() -> None:
    url = sa_url.make_url("dqlite://h:9001/db")
    assert _dqlite_generate_driver_url(url, "totally_unknown", None) is None


def test_known_drivers_each_return_rewritten_url() -> None:
    url = sa_url.make_url("dqlite://h:9001/db")
    for driver in _DRIVERNAMES:
        out = _dqlite_generate_driver_url(url, driver, None)
        assert out is not None, f"driver {driver!r} returned None"


def test_dqlitedbapi_maps_to_bare_dqlite_drivername() -> None:
    """``_format_url`` collapses ``dqlitedbapi`` to the bare ``dqlite`` drivername."""
    url = sa_url.make_url("dqlite://h:9001/db")
    out = _dqlite_generate_driver_url(url, "dqlitedbapi", None)
    assert out is not None
    assert out.drivername == "dqlite"


def test_aio_driver_yields_dqlite_aio_drivername() -> None:
    """``aio`` resolves to the explicit ``dqlite+aio`` drivername."""
    url = sa_url.make_url("dqlite://h:9001/db")
    out = _dqlite_generate_driver_url(url, "aio", None)
    assert out is not None
    assert out.drivername == "dqlite+aio"


def test_bare_dqlite_alias_rejected_to_preserve_fail_fast() -> None:
    """``driver="dqlite"`` is not allowlisted, so a ``dqlite+dqlite://`` typo
    fails fast instead of silently routing as the bare form."""
    url = sa_url.make_url("dqlite+dqlite://h:9001/db")
    assert _dqlite_generate_driver_url(url, "dqlite", None) is None


def test_drivernames_only_contains_sa_invoked_values() -> None:
    """The allowlist is exactly the two drivernames SA returns for this dialect."""
    assert frozenset({"dqlitedbapi", "aio"}) == _DRIVERNAMES


def _base() -> sa_url.URL:
    return sa_url.make_url("dqlite://127.0.0.1:9001/test")


def test_ident_with_lf_replaced() -> None:
    """LF must not survive into the database name; in a logged URL it splits
    the record."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\nINJECT")
    assert out.database is not None
    assert "\n" not in out.database
    assert "INJECT" in out.database  # body preserved, LF replaced


def test_ident_with_cr_replaced() -> None:
    """CR is the other half of CRLF log-record splitting."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\rINJECT")
    assert out.database is not None
    assert "\r" not in out.database


def test_ident_with_tab_replaced() -> None:
    """TAB is a C0 control character; structured loggers using TSV
    encode it as a column separator."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\tx")
    assert out.database is not None
    assert "\t" not in out.database


def test_ident_with_nul_replaced() -> None:
    """NUL passes through to the wire layer where ``encode_text``
    late-rejects it with an obscure error; reject it at the source."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x00x")
    assert out.database is not None
    assert "\x00" not in out.database


def test_ident_with_backslash_replaced() -> None:
    """The comment promises "path-separators" plural; the unix-only
    forward-slash strip was incomplete."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\\evil")
    assert out.database is not None
    assert "\\" not in out.database


def test_ident_with_u2028_replaced() -> None:
    """U+2028 is LINE SEPARATOR — LF-equivalent in journald and many
    JSON log encoders."""
    out = _format_url(_base(), "dqlitedbapi", "gw0 x")
    assert out.database is not None
    assert " " not in out.database


def test_ident_with_u2029_replaced() -> None:
    """U+2029 is PARAGRAPH SEPARATOR — same hazard as U+2028."""
    out = _format_url(_base(), "dqlitedbapi", "gw0 x")
    assert out.database is not None
    assert " " not in out.database


def test_ident_with_at_replaced() -> None:
    """Regression pin for the original ``@`` strip."""
    out = _format_url(_base(), "dqlitedbapi", "gw0@host")
    assert out.database is not None
    assert "@" not in out.database


def test_ident_with_slash_replaced() -> None:
    """Regression pin for the original ``/`` strip."""
    out = _format_url(_base(), "dqlitedbapi", "gw0/foo")
    assert out.database is not None
    assert "/" not in out.database


def test_ident_ordinary_alphanumeric_unchanged() -> None:
    """``gw0`` — the pytest-xdist convention — survives verbatim."""
    out = _format_url(_base(), "dqlitedbapi", "gw0")
    assert out.database is not None
    assert "_gw0" in out.database


def test_ident_with_dot_preserved() -> None:
    """Not a strict alphanumeric allowlist — version suffixes like ``gw0.1`` survive."""
    out = _format_url(_base(), "dqlitedbapi", "gw0.1")
    assert out.database is not None
    assert "gw0.1" in out.database


def test_ident_with_hyphen_preserved() -> None:
    """Hyphens in idents (custom fixture configs) survive."""
    out = _format_url(_base(), "dqlitedbapi", "worker-a")
    assert out.database is not None
    assert "worker-a" in out.database


def test_ident_with_del_replaced() -> None:
    """DEL (``\\x7f``) is a ``Cc`` control char and shares the C0 TTY hazards."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x7finject")
    assert out.database is not None
    assert "\x7f" not in out.database


def test_ident_with_nel_replaced() -> None:
    """NEL (``\\x85``) is a line terminator for ``str.splitlines`` and log forwarders."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x85inject")
    assert out.database is not None
    assert "\x85" not in out.database


def test_ident_with_c1_low_replaced() -> None:
    """C1 lower boundary (``\\x80``); the scrub covers the full ``Cc`` category."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x80inject")
    assert out.database is not None
    assert "\x80" not in out.database


def test_ident_with_c1_high_replaced() -> None:
    """C1 upper boundary (``\\x9f``): last byte of the extended control range."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x9finject")
    assert out.database is not None
    assert "\x9f" not in out.database


def test_ident_already_suffixed_path_also_sanitised() -> None:
    """The already-suffixed branch (``_SESSION_TOKEN`` present) applies the same scrub."""
    from sqlalchemydqlite.provision import _SESSION_TOKEN

    pre_suffixed = sa_url.make_url(f"dqlite://127.0.0.1:9001/test_{_SESSION_TOKEN}")
    out = _format_url(pre_suffixed, "dqlitedbapi", "gw0\nx")
    assert out.database is not None
    assert "\n" not in out.database


def test_session_token_is_module_level_constant() -> None:
    """Captured once at import; a function-local would break the
    ``_SESSION_TOKEN in database`` idempotence check in ``_format_url``."""
    assert isinstance(p._SESSION_TOKEN, str)
    assert p._SESSION_TOKEN == p._SESSION_TOKEN


def test_session_token_shape_is_sa_pid_monotonicns() -> None:
    """Token shape ``sa_<pid>_<monotonic-ns>``: safe across dbapi/URL/wire layers."""
    assert re.match(r"^sa_\d+_\d+$", p._SESSION_TOKEN), (
        f"token shape must be 'sa_<pid>_<monotonic-ns>'; got {p._SESSION_TOKEN!r}"
    )


def test_session_token_embeds_importing_process_pid() -> None:
    """In the importing process the token's pid matches ``os.getpid()``
    (under fork-based xdist a worker's token still names the controller's pid)."""
    pid_component = int(p._SESSION_TOKEN.split("_")[1])
    assert pid_component == os.getpid()


def test_upsert_provision_hook_registered_for_dqlite() -> None:
    from sqlalchemy.testing.provision import upsert

    assert "dqlite" in upsert.fns, (
        "upsert hook for ``dqlite`` not registered; SA OnConflictTest / "
        "third-party introspection by backend name will silently miss "
        "our dialect"
    )


def test_upsert_provision_hook_returns_sqlite_insert_variant() -> None:
    """Re-uses ``sqlalchemy.dialects.sqlite.insert`` so the inherited
    ``SQLiteCompiler.visit_on_conflict_do_*`` path runs at compile."""
    from sqlalchemy import Column, Integer, MetaData, String, Table
    from sqlalchemy.dialects.sqlite import Insert as SQLiteInsert
    from sqlalchemy.testing.provision import upsert

    md = MetaData()
    t = Table(
        "t",
        md,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )
    fn = upsert.fns["dqlite"]
    stmt = fn(None, t, [t.c.id], set_lambda=lambda excluded: {"name": excluded.name})
    assert isinstance(stmt, SQLiteInsert)


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


def test_drop_user_tables_per_drop_failure_sanitises_exception_and_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Both the exception text and the table name must be LF-escaped."""
    forged_name = "evil\nFORGED: spoofed log line"
    forged_exc = "drop failed\nFORGED: spoofed exc"

    def _exec(sql: str, *args: Any) -> Any:
        if sql.startswith("SELECT name") and args:
            (params,) = args
            result = MagicMock()
            if params[0] == "table":
                result.fetchall.return_value = [(forged_name,)]
            else:
                result.fetchall.return_value = []
            return result
        if sql.startswith("SELECT count"):
            result = MagicMock()
            result.scalar.return_value = 0
            return result
        if sql.startswith("DROP"):
            raise RuntimeError(forged_exc)
        return MagicMock()

    conn = MagicMock()
    conn.exec_driver_sql.side_effect = _exec
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None
    eng = MagicMock()
    eng.connect.return_value = cm

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    matching = [rec for rec in caplog.records if "on DROP" in rec.getMessage()]
    assert matching, "per-drop DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got: {msg!r}"


def test_drop_user_tables_connect_failure_sanitises_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A forged LF in the connect exception message must be escaped."""
    forged = "connect failed\nFORGED: spoofed log line"

    eng = MagicMock()
    eng.connect.side_effect = RuntimeError(forged)

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    matching = [rec for rec in caplog.records if "during connect/exec" in rec.getMessage()]
    assert matching, "connect-failure DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got: {msg!r}"


def _inner(fn: Any) -> Any:
    """Reach the dqlite-specific inner function past SA's dispatcher."""
    return fn.fns["dqlite"]


_U2028 = " "


def test_create_db_log_sanitises_ident_strips_u2028(
    caplog: pytest.LogCaptureFixture,
) -> None:
    forged = f"tenant-a{_U2028}FAKE LOG INJECTION"
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")
    _inner(_dqlite_create_db)(MagicMock(), MagicMock(), forged)

    records = [r for r in caplog.records if "create_db" in r.getMessage()]
    assert records, f"expected create_db INFO record; got {caplog.records!r}"
    assert _U2028 not in records[0].getMessage(), (
        f"U+2028 must be stripped from create_db log; got {records[0].getMessage()!r}"
    )


def test_drop_db_log_sanitises_ident_strips_u2028(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    forged = f"tenant-b{_U2028}FAKE LOG INJECTION"
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")
    monkeypatch.setattr("sqlalchemydqlite.provision._drop_user_tables", lambda _eng: None)
    _inner(_dqlite_drop_db)(MagicMock(), MagicMock(), forged)

    records = [r for r in caplog.records if "drop_db" in r.getMessage()]
    assert records, f"expected drop_db INFO record; got {caplog.records!r}"
    assert _U2028 not in records[0].getMessage(), (
        f"U+2028 must be stripped from drop_db log; got {records[0].getMessage()!r}"
    )


def test_reap_dbs_info_log_masks_url_password(caplog: Any) -> None:
    """A password renders as ``***``; the literal password must not appear."""
    url = URL.create(
        drivername="dqlite+aio",
        username="admin",
        password="s3cret",
        host="cluster.example.com",
        port=9001,
        database="main",
    )
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs(url, [])

    matching = [r for r in caplog.records if "dqlite reap_dbs:" in r.getMessage()]
    assert matching, "reap_dbs INFO entry-line not emitted"
    msg = matching[0].getMessage()
    assert "s3cret" not in msg, f"password leaked into INFO log: {msg!r}"
    assert "***" in msg, f"hide_password mask missing from log: {msg!r}"


def test_reap_dbs_info_log_escapes_lf_in_host(caplog: Any) -> None:
    """An LF in the host is escaped as ``\\n``, not survived raw."""
    malformed = URL.create(
        drivername="dqlite",
        host="evil.example.com\n[CRITICAL] forged",
        port=9001,
        database="main",
    )
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs(malformed, [])

    matching = [r for r in caplog.records if "dqlite reap_dbs:" in r.getMessage()]
    assert matching, "reap_dbs INFO entry-line not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into INFO log (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got {msg!r}"


def test_reap_dbs_info_log_credential_free_url_renders_cleanly(caplog: Any) -> None:
    """A credential-free URL renders host:port cleanly with no spurious ``***``."""
    url = URL.create(
        drivername="dqlite",
        host="cluster.example.com",
        port=9001,
        database="main",
    )
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs(url, [])

    matching = [r for r in caplog.records if "dqlite reap_dbs:" in r.getMessage()]
    assert matching, "reap_dbs INFO entry-line not emitted"
    msg = matching[0].getMessage()
    assert "cluster.example.com" in msg
    assert "9001" in msg
    assert "***" not in msg, "hide_password mask should not appear absent a password"


def test_reap_dbs_debug_log_sanitises_exception_message(monkeypatch: Any, caplog: Any) -> None:
    """Forged LF in the bubbled-up exception is escaped as ``\\n`` in the record."""

    forged = "dropped\nFORGED: forged test-log entry"

    def _boom(_eng: Any) -> None:
        raise RuntimeError(forged)

    monkeypatch.setattr(provision, "_drop_user_tables", _boom)

    def _fake_create_engine(_url: Any) -> Any:
        class _E:
            def dispose(self) -> None:
                pass

        return _E()

    import sqlalchemy

    monkeypatch.setattr(sqlalchemy, "create_engine", _fake_create_engine)

    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs("dqlite:///somewhere", ["worker_1"])

    matching = [rec for rec in caplog.records if "reap_dbs ident=" in rec.getMessage()]
    assert matching, "reap_dbs DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, (
        f"sanitize_for_log should escape LF as the two-byte sequence backslash+n; got: {msg!r}"
    )


def test_safe_for_log_truncates_large_input() -> None:
    huge = "x" * (1024 * 1024)  # 1 MiB
    result = _safe_for_log(huge)
    # Generous 4 KiB bound shows truncation fired without coupling to the exact cap.
    assert len(result) < 4096, (
        f"safe_for_log must truncate large server-supplied input; got "
        f"len={len(result)} for 1 MiB input"
    )
    assert "truncated" in result


def test_safe_for_log_preserves_small_input() -> None:
    small = "my_table"
    assert _safe_for_log(small) == small


def test_safe_for_log_sanitises_control_chars() -> None:
    # U+2028 LINE SEPARATOR: repr() doesn't escape it and journald treats it as a
    # record separator; the sanitiser strips it.
    sanitised = _safe_for_log("foo bar")
    assert " " not in sanitised


def test_safe_for_log_cap_is_documented_constant() -> None:
    assert _LOG_TRUNCATE_MAX_CHARS == 200
