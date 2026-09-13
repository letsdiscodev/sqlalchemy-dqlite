"""Dialect contract: URL/connect args, isolation, disconnect, close/begin, compiler, DDL gates."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, create_engine, func, select
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateTable

import dqliteclient.exceptions as client_exc
import dqlitedbapi
import dqlitedbapi.exceptions as dbapi_exc
from dqliteclient import parse_address
from dqlitedbapi.exceptions import InterfaceError, NotSupportedError
from dqlitedbapi.exceptions import InterfaceError as DbapiInterfaceError
from dqlitewire import LEADER_ERROR_CODES, SQLITE_CORRUPT
from sqlalchemydqlite import DqliteDialect, DqliteDialect_aio, dialect, dialect_aio
from sqlalchemydqlite.base import DqliteCompiler


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


def test_sync_dialect_inherits_from_sqlite_dialect_pysqlite() -> None:
    assert issubclass(DqliteDialect, SQLiteDialect_pysqlite), (
        f"DqliteDialect must inherit from SQLiteDialect_pysqlite to receive "
        f"the canonical SA SQLite-dialect defaults; current MRO: "
        f"{[c.__name__ for c in DqliteDialect.__mro__]}"
    )


def test_async_dialect_inherits_from_sync_dqlite_dialect() -> None:

    assert issubclass(DqliteDialect_aio, SQLiteDialect_pysqlite)
    assert issubclass(DqliteDialect_aio, DqliteDialect)


class TestDialectInitEagerAutocommitRejection:
    def test_init_rejects_isolation_level_autocommit_eagerly(self) -> None:
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect(isolation_level="AUTOCOMMIT")

    def test_init_rejects_isolation_level_autocommit_eagerly_aio(self) -> None:
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect_aio(isolation_level="AUTOCOMMIT")

    @pytest.mark.parametrize("value", ["autocommit", "AutoCommit", "AUTOCOMMIT"])
    def test_init_rejects_case_variants(self, value: str) -> None:
        # SA accepts only the spaceless AUTOCOMMIT form (case-insensitive); match that
        # narrow form so we don't drift narrower than SA's own filter.
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect(isolation_level=value)

    def test_init_accepts_isolation_level_serializable(self) -> None:
        dialect = DqliteDialect(isolation_level="SERIALIZABLE")
        assert dialect._on_connect_isolation_level == "SERIALIZABLE"

    def test_init_accepts_no_isolation_level(self) -> None:
        dialect = DqliteDialect()
        assert dialect._on_connect_isolation_level is None


def _connect_kwargs(host: str, port: int = 9001) -> dict[str, object]:
    url = URL.create("dqlite", host=host, port=port, database="test")
    _, kwargs = DqliteDialect().create_connect_args(url)
    return kwargs


def test_ipv6_loopback_address_is_bracketed() -> None:
    kwargs = _connect_kwargs("::1", 9001)
    assert kwargs["address"] == "[::1]:9001", (
        f"IPv6 host must be bracketed before passing to dbapi; got {kwargs['address']!r}"
    )
    assert parse_address(str(kwargs["address"])) == ("::1", 9001)


def test_ipv6_full_address_is_bracketed() -> None:
    kwargs = _connect_kwargs("2001:db8::1", 9001)
    assert kwargs["address"] == "[2001:db8::1]:9001"
    assert parse_address(str(kwargs["address"])) == ("2001:db8::1", 9001)


def test_ipv6_global_unicast_is_bracketed() -> None:
    kwargs = _connect_kwargs("2001:db8:85a3::8a2e:370:7334", 9001)
    assert kwargs["address"] == "[2001:db8:85a3::8a2e:370:7334]:9001"


def test_ipv4_address_unchanged() -> None:
    kwargs = _connect_kwargs("127.0.0.1", 9001)
    assert kwargs["address"] == "127.0.0.1:9001"


def test_dns_hostname_unchanged() -> None:
    kwargs = _connect_kwargs("node1.example.com", 9001)
    assert kwargs["address"] == "node1.example.com:9001"


def test_default_localhost_unchanged() -> None:
    url = URL.create("dqlite", database="test")
    _, kwargs = DqliteDialect().create_connect_args(url)
    assert kwargs["address"] == "localhost:9001"


@pytest.mark.parametrize(
    "ipv6_host",
    [
        "::1",
        # ``::`` (unspecified) omitted: ``parse_address`` rejects it (TCP can't target it).
        "2001:db8::1",
        "fe80::1",
        "2001:db8:85a3::8a2e:370:7334",
    ],
)
def test_ipv6_addresses_round_trip_through_dbapi_parser(ipv6_host: str) -> None:
    kwargs = _connect_kwargs(ipv6_host, 9001)
    parsed_host, parsed_port = parse_address(str(kwargs["address"]))
    assert parsed_host == ipv6_host
    assert parsed_port == 9001


class TestCreateConnectArgsRejectsCredentials:
    def test_username_only_rejected(self) -> None:
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="db",
            username="svc",
        )
        with pytest.raises(ArgumentError, match="username or password"):
            DqliteDialect().create_connect_args(url)

    def test_password_only_rejected(self) -> None:
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="db",
            password="not-a-real-secret",
        )
        with pytest.raises(ArgumentError, match="username or password"):
            DqliteDialect().create_connect_args(url)

    def test_both_rejected(self) -> None:
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="db",
            username="svc",
            password="not-a-real-secret",
        )
        with pytest.raises(ArgumentError, match="username or password"):
            DqliteDialect().create_connect_args(url)

    def test_no_credentials_accepted(self) -> None:
        url = URL.create(drivername="dqlite", host="localhost", port=9001, database="db")
        _, kwargs = DqliteDialect().create_connect_args(url)
        assert kwargs["address"] == "localhost:9001"
        assert kwargs["database"] == "db"


class TestUserinfoStructuralRejection:
    @pytest.mark.parametrize(
        "bad_url",
        [
            "dqlite://@localhost:9001/db",
            "dqlite://:@localhost:9001/db",
            "dqlite://user@localhost:9001/db",
            "dqlite://user:pass@localhost:9001/db",
        ],
    )
    def test_userinfo_in_url_rejected(self, bad_url: str) -> None:
        with pytest.raises(ArgumentError, match="username|password"):
            create_engine(bad_url)


class TestUrlFragmentRejection:
    @pytest.mark.parametrize(
        "bad_url",
        [
            "dqlite://localhost:9001/db#frag",
            "dqlite://localhost:9001/db?timeout=5#frag",
            "dqlite://localhost:9001/db?max_total_rows=100#x",
        ],
    )
    def test_fragment_in_url_rejected(self, bad_url: str) -> None:
        with pytest.raises(ArgumentError, match="fragment"):
            create_engine(bad_url)


class TestHostShapePreValidation:
    @pytest.mark.parametrize(
        "bad_url",
        [
            "dqlite://münchen.example.com:9001/db",
            "dqlite://host1,host2:9001/db",
        ],
    )
    def test_invalid_host_shape_rejected_at_construction(self, bad_url: str) -> None:
        """Bad host shape surfaces as ArgumentError at create_engine, not deferred to connect()."""
        with pytest.raises(ArgumentError, match="host|address|hostname"):
            create_engine(bad_url)

    def test_valid_host_passes(self) -> None:
        """Ordinary hosts still construct."""
        eng = create_engine("dqlite://localhost:9001/db")
        assert eng is not None
        eng.dispose()


def test_url_query_busy_timeout_forwards_to_connect_kwargs() -> None:
    """``?busy_timeout=30.0`` ends up in the dialect's connect kwargs."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "30.0"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("busy_timeout") == 30.0


def test_url_query_busy_timeout_zero_accepted() -> None:
    """``?busy_timeout=0`` ("no retry") is accepted (stdlib parity)."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "0"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("busy_timeout") == 0.0


def test_url_query_busy_timeout_negative_rejected() -> None:
    """Negative values raise ``ArgumentError`` at URL-parse time."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "-1"},
    )
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_url_query_busy_timeout_non_numeric_rejected() -> None:
    """``?busy_timeout=abc`` raises at URL-parse time (converter is
    ``float``)."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "abc"},
    )
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_connect_args_busy_timeout_accepted() -> None:
    """``connect_args={"busy_timeout": N}`` works — kwarg is allowlisted."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": 30.0},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_busy_timeout_zero_accepted() -> None:
    """Zero accepted via connect_args= same as URL."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": 0.0},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_busy_timeout_negative_rejected() -> None:
    """Negative busy_timeout in connect_args= is rejected at first
    checkout (validator runs in the dialect's connect() override)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": -1.0},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_connect_args_busy_timeout_bool_rejected() -> None:
    """bool is rejected (would silently coerce True→1.0)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": True},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_url_query_check_same_thread_false_forwards_to_kwargs() -> None:
    """``?check_same_thread=false`` parses to the kwarg."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "false"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is False


def test_url_query_check_same_thread_true_forwards_to_kwargs() -> None:
    """``?check_same_thread=true`` parses to the kwarg."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "true"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is True


@pytest.mark.parametrize(
    "token,expected",
    [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("1", True),
        ("yes", True),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        ("0", False),
        ("no", False),
    ],
)
def test_url_query_bool_tokens_accepted(token: str, expected: bool) -> None:
    """The URL bool parser accepts the standard token set
    case-insensitively (shared ``_parse_url_bool`` helper)."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": token},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is expected


def test_url_query_invalid_token_rejected() -> None:
    """``?check_same_thread=notabool`` raises at URL-parse time."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "notabool"},
    )
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_connect_args_check_same_thread_false_accepted() -> None:
    """``connect_args={"check_same_thread": False}`` works."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": False},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_check_same_thread_true_accepted() -> None:
    """Explicit ``True`` also works (no-op vs default)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": True},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_check_same_thread_rejects_int_zero() -> None:
    """Strict bool: ``0`` is not ``False`` for this kwarg
    (mirrors the dbapi-side strict-bool validation)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": 0},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_connect_args_check_same_thread_rejects_string() -> None:
    """Strict bool: string ``"false"`` rejected via the dbapi
    surface (the SA validator catches non-bool too)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": "false"},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_url_query_combined_with_other_knobs() -> None:
    """Combine check_same_thread with other URL knobs (busy_timeout)
    to confirm they don't interfere."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "false", "busy_timeout": "10.0"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is False
    assert kwargs.get("busy_timeout") == 10.0


def _extract_connect_args(engine: object) -> dict[str, object]:
    args, kwargs = engine.dialect.create_connect_args(engine.url)  # type: ignore[attr-defined]
    return dict(kwargs)


def test_url_query_max_message_size_propagates() -> None:
    engine = create_engine(
        "dqlite://localhost:9001/test?max_message_size=12345",
    )
    try:
        kwargs = _extract_connect_args(engine)
        assert kwargs.get("max_message_size") == 12345
    finally:
        engine.dispose()


def test_url_query_max_message_size_none_propagates() -> None:
    """``?max_message_size=none`` (use wire default) must propagate as ``None``."""
    engine = create_engine(
        "dqlite://localhost:9001/test?max_message_size=none",
    )
    try:
        kwargs = _extract_connect_args(engine)
        assert kwargs.get("max_message_size") is None
    finally:
        engine.dispose()


def test_url_query_max_message_size_negative_rejected() -> None:
    """Negative values rejected at URL-parse time, not deferred to first checkout."""
    with pytest.raises(ArgumentError):
        create_engine("dqlite://localhost:9001/test?max_message_size=-1")


def test_url_query_max_message_size_non_int_rejected() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://localhost:9001/test?max_message_size=abc")


def test_connect_args_max_message_size_propagates() -> None:
    """``connect_args=`` merges into dbapi.connect() kwargs at runtime, not URL-parse."""
    captured: dict[str, object] = {}

    engine = create_engine(
        "dqlite://localhost:9001/test",
        connect_args={"max_message_size": 54321},
    )
    try:
        real_connect = engine.dialect.loaded_dbapi.connect

        def _capturing_connect(*args: object, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("propagation-check stub; no real connect")

        engine.dialect.loaded_dbapi.connect = _capturing_connect  # type: ignore[attr-defined]
        try:
            with pytest.raises(RuntimeError, match="propagation-check stub"):
                engine.connect()
        finally:
            engine.dialect.loaded_dbapi.connect = real_connect  # type: ignore[attr-defined]
        assert captured.get("max_message_size") == 54321
    finally:
        engine.dispose()


def test_async_url_query_max_message_size_propagates() -> None:
    engine = create_async_engine(
        "dqlite+aio://localhost:9001/test?max_message_size=99999",
    )
    try:
        kwargs = _extract_connect_args(engine.sync_engine)
        assert kwargs.get("max_message_size") == 99999
    finally:
        # Never connected, so sync dispose of the inner engine is a safe fallback.
        engine.sync_engine.dispose()


def test_async_connect_args_max_message_size_propagates() -> None:
    """Same connect_args runtime-merge check on the async dialect."""
    captured: dict[str, object] = {}

    engine = create_async_engine(
        "dqlite+aio://localhost:9001/test",
        connect_args={"max_message_size": 77777},
    )
    try:
        sync_engine = engine.sync_engine
        real_connect = sync_engine.dialect.loaded_dbapi.connect

        def _capturing_connect(*args: object, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("propagation-check stub; no real connect")

        sync_engine.dialect.loaded_dbapi.connect = _capturing_connect  # type: ignore[attr-defined]
        try:
            with pytest.raises(RuntimeError, match="propagation-check stub"):
                sync_engine.connect()
        finally:
            sync_engine.dialect.loaded_dbapi.connect = real_connect  # type: ignore[attr-defined]
        assert captured.get("max_message_size") == 77777
    finally:
        engine.sync_engine.dispose()


def test_reset_isolation_level_is_defined_locally() -> None:
    """Local override guards against an SA refactor silently regressing the contract."""
    assert "reset_isolation_level" in DqliteDialect.__dict__, (
        "DqliteDialect.reset_isolation_level must be a local override "
        "(not inherited) so SA's pool checkin path does not route "
        "through _assert_and_set_isolation_level + set_isolation_level "
        "and surface the AUTOCOMMIT rejection on a finalize path."
    )


def test_reset_isolation_level_is_noop_and_does_not_dispatch() -> None:
    """The override must not call ``set_isolation_level`` (which would re-fire
    the AUTOCOMMIT rejection)."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    mock_conn = MagicMock()
    dialect.reset_isolation_level(mock_conn)
    assert not mock_conn.method_calls


def test_async_dialect_inherits_local_reset_isolation_level() -> None:
    """The async dialect inherits the override rather than duplicating it."""
    assert DqliteDialect_aio.reset_isolation_level is DqliteDialect.reset_isolation_level


class TestDetectAutocommitSetting:
    def test_dqlite_dialect_returns_false_on_arbitrary_object(self) -> None:
        # object() (not MagicMock, whose auto-attribute would mask it) exposes
        # the inherited probe's real failure mode.
        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(object()) is False  # type: ignore[arg-type]

    def test_dqlite_dialect_returns_false_on_magic_mock(self) -> None:
        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(MagicMock()) is False

    def test_dqlite_dialect_does_not_touch_dbapi_attribute(self) -> None:
        # A class-level descriptor (not MagicMock.side_effect, which fires only on
        # call) makes any read of isolation_level raise, surfacing an accidental probe.
        class _Probe:
            @property
            def isolation_level(self) -> object:
                raise AssertionError("must not probe isolation_level")

        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(_Probe()) is False  # type: ignore[arg-type]

    def test_async_dialect_inherits_override(self) -> None:
        dialect = DqliteDialect_aio()
        assert dialect.detect_autocommit_setting(object()) is False  # type: ignore[arg-type]

    def test_dbapi_connection_exposes_isolation_level_returning_none(self) -> None:
        """The dbapi Connection.isolation_level reads as None (stdlib parity stub),
        the value SA's probe keys on. Pin the value, not just the descriptor."""
        from dqlitedbapi.connection import Connection as DqliteSyncConnection

        conn = DqliteSyncConnection("localhost:19001")
        try:
            assert conn.isolation_level is None
        finally:
            conn.close()

    def test_async_dbapi_connection_isolation_level_also_returns_none(self) -> None:
        """Sibling pin on the async surface."""
        from dqlitedbapi.aio.connection import (
            AsyncConnection as DqliteAsyncConnection,
        )

        assert DqliteAsyncConnection("localhost:19001").isolation_level is None

    def test_override_remains_load_bearing_against_isolation_level_eq_none(self) -> None:
        """The override returns False even when the probe sees isolation_level is None."""

        class _StdlibParityProbe:
            isolation_level = None

        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(_StdlibParityProbe()) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "message",
    [
        "Connection is closed (id=140234123)",
        "Cursor is closed (id=140234123)",
        "cursor is closed (id=140234123)",
    ],
)
def test_is_disconnect_matches_closed_handle_with_id_suffix(message: str) -> None:
    err = DbapiInterfaceError(message)
    assert DqliteDialect().is_disconnect(err, None, None) is True, (
        f"is_disconnect classifier must still match the closed-handle "
        f"phrase even when enriched with (id=...) suffix: {message!r}"
    )


def _make_dialect() -> DqliteDialect:
    return DqliteDialect()


def test_used_after_fork_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "Connection used after fork; reconstruct from configuration "
        "in the target process. (created in pid 1234, current pid 5678)"
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_async_used_after_fork_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "AsyncConnection used after fork; reconstruct from "
        "configuration in the target process. (created in pid 1234, "
        "current pid 5678)"
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_unrelated_fork_word_not_classified_as_disconnect() -> None:
    """An InterfaceError with an unrelated "fork" must not trip."""
    dialect = _make_dialect()
    e = InterfaceError("forked workflow not supported by this trigger")
    assert dialect.is_disconnect(e, None, None) is False


# Verbatim copy of DqliteConnection.transaction()'s wording; a client
# wording change must update this string and re-verify classification.
_UNTRACKED_SAVEPOINT_MESSAGE = (
    "Cannot start transaction: a SAVEPOINT outside an explicit "
    "BEGIN is currently open on this connection (the SQLite "
    "engine has auto-begun a transaction). Issue COMMIT / "
    "ROLLBACK or RELEASE the outer SAVEPOINT before entering "
    "transaction()."
)


def test_untracked_savepoint_interfaceerror_not_classified_as_disconnect() -> None:
    dialect = DqliteDialect()
    exc = InterfaceError(_UNTRACKED_SAVEPOINT_MESSAGE)

    assert dialect.is_disconnect(exc, None, None) is False, (
        "untracked-SAVEPOINT InterfaceError reports a programmer "
        "mistake on a healthy connection — must NOT trigger pool "
        "invalidation. A regression here would mask the diagnostic "
        "with slot churn."
    )


def test_savepoint_substring_alone_does_not_classify_as_disconnect() -> None:
    """A bare InterfaceError mentioning "savepoint" must stay non-disconnect."""
    dialect = DqliteDialect()
    exc = InterfaceError("savepoint name parse error: bad identifier")

    assert dialect.is_disconnect(exc, None, None) is False


def test_connection_closed_interfaceerror_remains_classified_as_disconnect() -> None:
    """Positive control: "Connection is closed" must still classify."""
    dialect = DqliteDialect()
    exc = InterfaceError("Connection is closed")

    assert dialect.is_disconnect(exc, None, None) is True


def test_cursor_closed_interfaceerror_remains_classified_as_disconnect() -> None:
    """Positive control: "Cursor is closed" must still classify."""
    dialect = DqliteDialect()
    exc = InterfaceError("Cursor is closed")

    assert dialect.is_disconnect(exc, None, None) is True


def test_do_close_calls_dbapi_close_with_no_kwargs() -> None:
    """Happy path: ``close()`` is called with no arguments."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()


def test_do_close_calls_close_when_close_timeout_attr_missing() -> None:
    """close() is called the same way whether or not ``_close_timeout`` exists."""
    dialect = DqliteDialect()
    mock_conn = MagicMock(spec=["close"])
    mock_conn.close = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()


def test_do_close_does_not_fall_through_to_force_close_on_happy_path() -> None:
    """On the happy path ``force_close_transport`` is NOT called."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock()
    mock_conn.force_close_transport = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()
    mock_conn.force_close_transport.assert_not_called()


@pytest.mark.parametrize(
    "transport_exc",
    [
        TimeoutError("close timed out"),
        ConnectionResetError("close FIN"),
        OSError("close OSError"),
    ],
    ids=["TimeoutError", "ConnectionResetError", "OSError"],
)
def test_do_close_transport_failure_falls_through_to_force_close(
    transport_exc: BaseException,
) -> None:
    """Transport-class failure during close falls through to
    ``force_close_transport`` so the slot releases."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock(side_effect=transport_exc)
    mock_conn.force_close_transport = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()
    mock_conn.force_close_transport.assert_called_once_with()


def test_do_close_programmer_bug_raises_instead_of_force_close_fallback() -> None:
    """A programmer bug (AttributeError/TypeError) propagates rather than
    falling through to the ``force_close_transport`` fallback."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock(side_effect=AttributeError("refactor bug"))
    mock_conn.force_close_transport = MagicMock()

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_not_called()


def test_async_do_close_fallback_runs_in_calling_thread_not_loop() -> None:
    """``force_close_transport`` must run on the caller's thread, not a
    worker loop thread."""
    import threading
    from unittest.mock import MagicMock

    from dqliteclient.exceptions import DqliteConnectionError

    captured_thread: list[int] = []

    adapter = MagicMock()
    adapter.close.side_effect = DqliteConnectionError("transport")
    adapter.force_close_transport = MagicMock(
        side_effect=lambda: captured_thread.append(threading.get_ident())
    )

    caller_tid = threading.get_ident()
    DqliteDialect_aio().do_close(adapter)

    assert captured_thread == [caller_tid], (
        f"force_close_transport must run on the caller's thread "
        f"(sync teardown); captured {captured_thread}, caller {caller_tid}"
    )


def test_do_recover_twophase_still_raises_not_supported_error() -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    with pytest.raises(NotSupportedError, match="two-phase"):
        dialect.do_recover_twophase(MagicMock())


def test_do_begin_closes_cursor_on_cancel() -> None:
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = asyncio.CancelledError("greenlet cancel")
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(asyncio.CancelledError):
        dialect.do_begin(mock_conn)

    mock_cursor.close.assert_called_once_with()


def test_do_begin_does_not_mask_begin_exception_with_close_failure() -> None:
    """A transport-class close-time failure must not replace the BEGIN-time exception (the
    finally swallows it with a DEBUG log)."""
    from dqliteclient.exceptions import DqliteConnectionError
    from dqlitedbapi.exceptions import OperationalError

    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = OperationalError("leader flip during BEGIN")
    mock_cursor.close.side_effect = DqliteConnectionError("transport reset")
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(OperationalError, match="leader flip during BEGIN"):
        dialect.do_begin(mock_conn)


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
    """Negated form: SA dispatches to a separate
    ``visit_not_regexp_match_op_binary`` visitor — both must override."""
    from dqlitedbapi.exceptions import NotSupportedError

    stmt = select(stmt_table).where(~stmt_table.c.name.regexp_match("foo"))

    with pytest.raises(NotSupportedError):
        stmt.compile(dialect=DqliteDialect())


def test_async_dialect_inherits_dqlite_compiler() -> None:
    """The aio dialect inherits the statement-compiler binding."""
    assert DqliteDialect_aio.statement_compiler is DqliteCompiler
    assert DqliteDialect.statement_compiler is DqliteCompiler


def test_dqlite_compiler_subclasses_sqlite_compiler() -> None:
    """``DqliteCompiler`` subclasses ``SQLiteCompiler`` so the other
    SQLite compile rules (ON CONFLICT, RETURNING) are inherited."""
    from sqlalchemy.dialects.sqlite.base import SQLiteCompiler

    assert issubclass(DqliteCompiler, SQLiteCompiler)


def test_sa_func_floor_raises_not_supported_at_compile_time() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.floor(t.c.x)).compile(dialect=DqliteDialect()))


def test_sa_func_floor_diagnostic_names_the_workaround() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    try:
        str(select(func.floor(t.c.x)).compile(dialect=DqliteDialect()))
    except NotSupportedError as exc:
        msg = str(exc)
        assert "CAST" in msg or "client-side" in msg
        assert "DqliteCompiler" not in msg or "subclass" in msg


def test_other_funcs_still_compile() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    sql = str(select(func.count(t.c.x)).compile(dialect=DqliteDialect()))
    assert "count" in sql.lower()


def test_floor_case_insensitive_name_matching() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    for name in ("floor", "FLOOR", "Floor"):
        with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
            str(select(getattr(func, name)(t.c.x)).compile(dialect=DqliteDialect()))


def test_floor_inside_compound_expression_still_trips_gate() -> None:
    """A nested floor (inside coalesce/sum/etc.) must still trip the gate."""
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.coalesce(func.floor(t.c.x), 0)).compile(dialect=DqliteDialect()))


def test_floor_in_where_clause_trips_gate() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    stmt = select(t.c.x).where(func.floor(t.c.x) > 0)
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(stmt.compile(dialect=DqliteDialect()))


def test_floor_in_order_by_trips_gate() -> None:
    m = MetaData()
    t = Table("t", m, Column("x", Integer))

    stmt = select(t.c.x).order_by(func.floor(t.c.x))
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(stmt.compile(dialect=DqliteDialect()))


def test_floor_zero_args_still_trips_gate_before_arg_validation() -> None:
    """The gate fires on the name alone, before SA's arg-resolver masks it with a
    generic "wrong number of args" error."""
    with pytest.raises(NotSupportedError, match="SQLITE_ENABLE_MATH_FUNCTIONS"):
        str(select(func.floor()).compile(dialect=DqliteDialect()))


def test_table_dqlite_with_rowid_raises_argument_error() -> None:
    m = MetaData()
    with pytest.raises(ArgumentError) as exc_info:
        Table(
            "t_bad",
            m,
            Column("id", Integer, primary_key=True),
            dqlite_with_rowid=False,
        )
    msg = str(exc_info.value)
    assert "sqlite_with_rowid" in msg, msg
    assert "dqlite_" in msg, msg


def test_table_sqlite_with_rowid_still_works() -> None:
    m = MetaData()
    t = Table(
        "t_ok",
        m,
        Column("id", Integer, primary_key=True),
        sqlite_with_rowid=False,
    )
    assert t.dialect_options["sqlite"]["with_rowid"] is False


def test_table_without_dialect_kwargs_works() -> None:
    m = MetaData()
    Table("t_plain", m, Column("id", Integer, primary_key=True))


def test_index_dqlite_where_raises_argument_error() -> None:
    m = MetaData()
    t = Table("t_idx", m, Column("id", Integer, primary_key=True), Column("v", Integer))
    with pytest.raises(ArgumentError, match="sqlite_where"):
        Index("ix_bad", t.c.v, dqlite_where="v > 0")


def test_sqlite_with_rowid_kwarg_takes_effect() -> None:
    m = MetaData()
    t = Table(
        "t1",
        m,
        Column("id", Integer, primary_key=True),
        sqlite_with_rowid=False,
    )
    compiled = str(CreateTable(t).compile(dialect=DqliteDialect()))
    assert "WITHOUT ROWID" in compiled


def test_dqlite_with_rowid_kwarg_rejected_at_construction() -> None:
    """The dqlite_* DDL prefix is refused at construction with an ArgumentError."""
    from sqlalchemy.exc import ArgumentError

    m = MetaData()
    with pytest.raises(ArgumentError, match="sqlite_with_rowid"):
        Table(
            "t2",
            m,
            Column("id", Integer, primary_key=True),
            dqlite_with_rowid=False,
        )
