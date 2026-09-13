"""Dialect contract: URL parsing, connect args, isolation, disconnect classification."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions as client_exc
import dqlitedbapi
import dqlitedbapi.exceptions as dbapi_exc
from dqlitewire import LEADER_ERROR_CODES, SQLITE_CORRUPT
from sqlalchemydqlite import DqliteDialect, DqliteDialect_aio, dialect, dialect_aio


def test_identity_and_exports() -> None:
    assert dialect is DqliteDialect and dialect_aio is DqliteDialect_aio
    assert (DqliteDialect.name, DqliteDialect.driver) == ("dqlite", "dqlitedbapi")
    assert (DqliteDialect_aio.driver, DqliteDialect_aio.is_async) == ("aio", True)
    assert DqliteDialect.import_dbapi() is dqlitedbapi
    assert DqliteDialect_aio.import_dbapi() is dqlitedbapi.aio
    assert DqliteDialect().paramstyle == "qmark"
    for cls in (DqliteDialect, DqliteDialect_aio):
        assert cls.supports_statement_cache and cls.has_terminate
        assert not cls.supports_server_side_cursors


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("dqlite://", {"address": "localhost:9001", "database": "default"}),
        ("dqlite://h:9002/db", {"address": "h:9002", "database": "db"}),
        ("dqlite://[::1]:9001/db", {"address": "[::1]:9001", "database": "db"}),
        (
            "dqlite://h:1/db?timeout=2.5&max_total_rows=none",
            {"timeout": 2.5, "max_total_rows": None},
        ),
        (
            "dqlite://h:1/db?trust_server_heartbeat=true&session_mode=READ_ONLY",
            {"trust_server_heartbeat": True, "session_mode": "read_only"},
        ),
        (
            "dqlite://h:1/db?max_message_size=1024&busy_timeout=0",
            {"max_message_size": 1024, "busy_timeout": 0.0},
        ),
    ],
)
def test_create_connect_args(url: str, expected: dict[str, Any]) -> None:
    _, kwargs = DqliteDialect().create_connect_args(make_url(url))
    for key, value in expected.items():
        assert kwargs[key] == value


@pytest.mark.parametrize(
    "url",
    [
        "dqlite://user:pw@h:9001/db",
        "dqlite://h:70000/db",
        "dqlite://bad host:9001/db",
        "dqlite://h:9001/db?unknown=1",
        "dqlite://h:9001/db?timeout=0",
        "dqlite://h:9001/db?timeout=abc",
        "dqlite://h:9001/db?max_total_rows=0",
        "dqlite://h:9001/db?close_timeout=0.001",
        "dqlite://h:9001/db?trust_server_heartbeat=maybe",
        "dqlite://h:9001/db?session_mode=serializable",
    ],
)
def test_bad_urls_raise_argument_error(url: str) -> None:
    with pytest.raises(ArgumentError):
        DqliteDialect().create_connect_args(make_url(url))


def test_connect_validates_connect_args(monkeypatch: pytest.MonkeyPatch) -> None:
    d = DqliteDialect()
    d.dbapi = MagicMock()
    with pytest.raises(ArgumentError, match="Unknown dqlite connect kwarg"):
        d.connect(address="h:1", databse="typo")
    with pytest.raises(ArgumentError, match="out of range"):
        d.connect(address="h:1", timeout=True)
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        d.connect(address="h:1", isolation_level="AUTOCOMMIT")
    with pytest.raises(ArgumentError, match="must be callable"):
        d.connect(address="h:1", creator_fn="nope")
    d.connect(address="h:1", database="x", dial_func=lambda a: None)
    d.dbapi.connect.assert_called_once_with(
        address="h:1", database="x", dial_func=d.dbapi.connect.call_args.kwargs["dial_func"]
    )
    created = object()
    assert d.connect(address="h:1", creator_fn=lambda **kw: created) is created


def test_isolation_is_serializable_only() -> None:
    d = DqliteDialect()
    conn = MagicMock()
    assert d.get_isolation_level(conn) == "SERIALIZABLE"
    d.set_isolation_level(conn, "serializable")  # type: ignore[arg-type]
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        d.set_isolation_level(conn, "AUTOCOMMIT")
    with pytest.raises(ArgumentError):
        d.set_isolation_level(conn, "READ COMMITTED")
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        DqliteDialect(isolation_level="AUTOCOMMIT")
    with pytest.raises(ArgumentError, match="qmark"):
        DqliteDialect(paramstyle="named")
    with pytest.raises(ArgumentError, match="native_datetime"):
        DqliteDialect(native_datetime=True)
    assert d.detect_autocommit_setting(conn) is False
    assert d.on_connect() is None  # type: ignore[func-returns-value]


def test_do_begin_sends_bare_begin_and_closes_cursor() -> None:
    conn = MagicMock()
    conn.cursor.return_value.close.side_effect = client_exc.DqliteConnectionError("gone")
    DqliteDialect().do_begin(conn)
    conn.cursor.return_value.execute.assert_called_once_with("BEGIN")


def test_two_phase_is_not_supported() -> None:
    d = DqliteDialect()
    for call in (
        lambda: d.do_begin_twophase(None, "x"),
        lambda: d.do_prepare_twophase(None, "x"),
        lambda: d.do_commit_twophase(None, "x"),
        lambda: d.do_rollback_twophase(None, "x"),
        lambda: d.do_recover_twophase(None),
    ):
        with pytest.raises(dbapi_exc.NotSupportedError):
            call()


def _chain(outer: BaseException, inner: BaseException) -> BaseException:
    outer.__cause__ = inner
    return outer


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (client_exc.DqliteConnectionError("Connection closed by server"), True),
        (client_exc.ClusterError("Could not find leader"), True),
        (client_exc.ProtocolError("bad frame"), True),
        (client_exc.ClusterPolicyError("rejected"), False),
        (OSError("reset"), True),
        (
            _chain(dbapi_exc.OperationalError("wrapped"), client_exc.DqliteConnectionError("x")),
            True,
        ),
        (_chain(RuntimeError("app"), _chain(dbapi_exc.OperationalError("w"), OSError("x"))), True),
        (dbapi_exc.OperationalError("not leader", code=next(iter(LEADER_ERROR_CODES))), True),
        (dbapi_exc.AmbiguousCommitError("leadership lost", code=1), True),
        (dbapi_exc.OperationalError("server is shutting down", code=1), True),
        (dbapi_exc.OperationalError("Failed to connect: refused"), True),
        (dbapi_exc.OperationalError("Connection timed out"), True),
        (dbapi_exc.OperationalError("no such savepoint: sa_savepoint_1"), False),
        (dbapi_exc.OperationalError("constraint failed", code=19), False),
        (dbapi_exc.InterfaceError("Connection is closed (id=1)"), True),
        (
            dbapi_exc.InterfaceError("Connection invalidated (id=1): the wire session was lost"),
            True,
        ),
        (dbapi_exc.InterfaceError("AsyncConnection is bound to a different event loop"), True),
        (dbapi_exc.InterfaceError("Connection used after fork"), True),
        (dbapi_exc.InterfaceError("trace: different event loop seen at hop 3"), False),
        (dbapi_exc.ProgrammingError("bad params"), False),
        (dbapi_exc.DatabaseError("corrupt", code=SQLITE_CORRUPT), True),
        (dbapi_exc.IntegrityError("dup", code=SQLITE_CORRUPT), False),
        (BaseExceptionGroup("g", [ValueError("x"), client_exc.DqliteConnectionError("y")]), True),
    ],
)
def test_is_disconnect_classification(exc: BaseException, expected: bool) -> None:
    assert DqliteDialect().is_disconnect(exc, None, None) is expected


def test_is_disconnect_trusts_the_connection_state() -> None:
    conn = SimpleNamespace(invalidated=True, closed=False)
    assert DqliteDialect().is_disconnect(ValueError("anything"), conn, None) is True
    live = SimpleNamespace(invalidated=False, closed=False)
    assert DqliteDialect().is_disconnect(ValueError("anything"), live, None) is False
    wrapped = SimpleNamespace(_connection=SimpleNamespace(invalidated=False, closed=True))
    assert DqliteDialect().is_disconnect(ValueError("anything"), wrapped, None) is True


def test_do_close_falls_back_to_force_close() -> None:
    conn = MagicMock()
    conn.close.side_effect = dbapi_exc.OperationalError("lost")
    DqliteDialect().do_close(conn)
    conn.force_close_transport.assert_called_once_with()
    DqliteDialect().do_terminate(conn)
    assert conn.force_close_transport.call_count == 2


def test_compiler_refuses_udf_backed_constructs() -> None:
    engine = create_engine("dqlite://localhost:9001/db")
    from sqlalchemy import column

    with pytest.raises(dbapi_exc.NotSupportedError, match="regexp"):
        select(column("c")).where(column("c").regexp_match("x")).compile(engine)
    with pytest.raises(dbapi_exc.NotSupportedError, match="floor"):
        select(func.floor(column("c"))).compile(engine)
    assert "abs(c)" in str(select(func.abs(column("c"))).compile(engine))


def test_ddl_kwargs_must_use_sqlite_prefix() -> None:
    from sqlalchemy import Column, Integer, MetaData, Table

    with pytest.raises(ArgumentError, match="sqlite_with_rowid"):
        Table("t", MetaData(), Column("id", Integer, primary_key=True), dqlite_with_rowid=False)
    t = Table("t", MetaData(), Column("id", Integer, primary_key=True), sqlite_with_rowid=False)
    assert t.dialect_options["sqlite"]["with_rowid"] is False
