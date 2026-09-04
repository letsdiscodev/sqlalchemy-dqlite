"""Dialect configuration tests: do_ping exceptions, is_disconnect dispatch,
create_connect_args URL plumbing, and isolation-level handling."""

from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions
import dqlitedbapi.exceptions
from sqlalchemydqlite.base import DqliteDialect


class TestGetIsolationLevelValues:
    def test_only_serializable(self) -> None:
        """SERIALIZABLE plus AUTOCOMMIT (advertised only so SA routes it through our
        rejecting set_isolation_level); READ UNCOMMITTED is not, since it has no
        server-side effect."""
        dialect = DqliteDialect()
        values = list(dialect.get_isolation_level_values(MagicMock()))
        assert "SERIALIZABLE" in values
        assert "AUTOCOMMIT" in values
        assert "READ UNCOMMITTED" not in values

    def test_read_uncommitted_not_advertised(self) -> None:
        dialect = DqliteDialect()
        assert "READ UNCOMMITTED" not in dialect.get_isolation_level_values(MagicMock())

    def test_defined_locally(self) -> None:
        """Must be overridden locally; inheriting SQLiteDialect's would re-add READ UNCOMMITTED."""
        assert "get_isolation_level_values" in DqliteDialect.__dict__


class TestSetIsolationLevel:
    def test_serializable_is_noop(self) -> None:
        dialect = DqliteDialect()
        dialect.set_isolation_level(MagicMock(), "SERIALIZABLE")

    def test_autocommit_rejected(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            dialect.set_isolation_level(MagicMock(), "AUTOCOMMIT")

    def test_other_levels_rejected(self) -> None:
        """An unknown level raises ArgumentError rather than warn-and-coerce, which
        would silently change the caller's requested semantics."""
        dialect = DqliteDialect()
        with pytest.raises(ArgumentError, match="only supports SERIALIZABLE"):
            dialect.set_isolation_level(MagicMock(), "READ COMMITTED")


class TestCreateConnectArgsURLQuery:
    def test_timeout_forwarded(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?timeout=5.5")
        args, kwargs = dialect.create_connect_args(url)
        assert kwargs["timeout"] == 5.5
        assert kwargs["address"] == "host:19001"
        assert kwargs["database"] == "db"

    def test_no_query_params_still_works(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db")
        _, kwargs = dialect.create_connect_args(url)
        assert "timeout" not in kwargs

    def test_unknown_param_raises(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?timeoutt=5")  # typo
        with pytest.raises(ArgumentError, match="timeoutt"):
            dialect.create_connect_args(url)

    def test_unparseable_timeout_raises(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?timeout=not-a-number")
        with pytest.raises(ArgumentError, match="float"):
            dialect.create_connect_args(url)

    def test_max_total_rows_forwarded(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=5000")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] == 5000

    def test_max_total_rows_unparseable_raises(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=not-an-int")
        with pytest.raises(ArgumentError, match="int"):
            dialect.create_connect_args(url)

    @pytest.mark.parametrize("bad_port", [0, -1, 65536, 70000])
    def test_invalid_port_raises(self, bad_port: int) -> None:
        """An out-of-range port smuggled past SA's parser must still fail at
        create_connect_args time."""
        from sqlalchemy.engine import URL

        dialect = DqliteDialect()
        url = URL.create("dqlite", host="host", port=bad_port, database="db")
        with pytest.raises(ArgumentError, match="out of the valid"):
            dialect.create_connect_args(url)

    @pytest.mark.parametrize("good_port", [1, 9001, 65535])
    def test_valid_port_accepted(self, good_port: int) -> None:
        from sqlalchemy.engine import URL

        dialect = DqliteDialect()
        url = URL.create("dqlite", host="host", port=good_port, database="db")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["address"] == f"host:{good_port}"

    def test_timeout_and_max_total_rows_together(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?timeout=3.5&max_total_rows=250")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["timeout"] == 3.5
        assert kwargs["max_total_rows"] == 250

    def test_max_continuation_frames_forwarded(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_continuation_frames=500")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_continuation_frames"] == 500

    def test_max_continuation_frames_rejects_non_int(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_continuation_frames=nope")
        with pytest.raises(ArgumentError, match="int"):
            dialect.create_connect_args(url)

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("1", True),
            ("true", True),
            ("True", True),
            ("YES", True),
            ("on", True),
            ("0", False),
            ("false", False),
            ("no", False),
            ("off", False),
        ],
    )
    def test_trust_server_heartbeat_parses_boolean(self, raw: str, expected: bool) -> None:
        """URL values arrive as strings; bool("False") is truthy, so a dedicated parser is used."""
        dialect = DqliteDialect()
        url = make_url(f"dqlite://host:19001/db?trust_server_heartbeat={raw}")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["trust_server_heartbeat"] is expected

    @pytest.mark.parametrize(
        "raw",
        ["enabled", "flase", "yse", "2", "maybe"],
    )
    def test_trust_server_heartbeat_rejects_unknown_tokens(self, raw: str) -> None:
        """Unknown tokens must raise rather than silently coerce to False (a typo would
        otherwise leave the operator with the default they thought they overrode)."""
        dialect = DqliteDialect()
        url = make_url(f"dqlite://host:19001/db?trust_server_heartbeat={raw}")
        with pytest.raises(ArgumentError, match="Invalid bool value"):
            dialect.create_connect_args(url)

    def test_close_timeout_forwarded(self) -> None:
        """close_timeout URL plumbing so operators can tune the engine.dispose() drain budget."""
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?close_timeout=2.5")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["close_timeout"] == 2.5

    def test_close_timeout_rejects_non_float(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?close_timeout=nope")
        with pytest.raises(ArgumentError, match="float"):
            dialect.create_connect_args(url)

    @pytest.mark.parametrize("raw", ["0", "-1", "nan", "inf", "-inf"])
    def test_close_timeout_rejects_out_of_range(self, raw: str) -> None:
        dialect = DqliteDialect()
        url = make_url(f"dqlite://host:19001/db?close_timeout={raw}")
        with pytest.raises(ArgumentError, match="close_timeout"):
            dialect.create_connect_args(url)

    def test_close_timeout_below_floor_carries_fin_flush_rationale(self) -> None:
        """A below-floor close_timeout surfaces the client-layer FIN-flush rationale:
        the URL validator delegates to validate_timeout so the message is single-sourced."""
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?close_timeout=0.0001")
        with pytest.raises(ArgumentError) as exc:
            dialect.create_connect_args(url)
        assert "FIN flushes" in str(exc.value)
        assert "close_timeout" in str(exc.value)


class TestURLQueryRangeValidation:
    @pytest.mark.parametrize(
        "url",
        [
            "dqlite://host:19001/db?max_total_rows=0",
            "dqlite://host:19001/db?max_total_rows=-1",
            "dqlite://host:19001/db?max_continuation_frames=0",
            "dqlite://host:19001/db?max_continuation_frames=-5",
            "dqlite://host:19001/db?timeout=0",
            "dqlite://host:19001/db?timeout=-1.5",
            "dqlite://host:19001/db?timeout=nan",
            "dqlite://host:19001/db?timeout=inf",
            "dqlite://host:19001/db?timeout=-inf",
        ],
    )
    def test_invalid_range_rejected_at_parse_time(self, url: str) -> None:
        dialect = DqliteDialect()
        with pytest.raises(ArgumentError, match="out of range"):
            dialect.create_connect_args(make_url(url))

    @pytest.mark.parametrize(
        "url",
        [
            "dqlite://host:19001/db?max_total_rows=1",
            "dqlite://host:19001/db?max_total_rows=10000000",
            "dqlite://host:19001/db?max_continuation_frames=1",
            "dqlite://host:19001/db?timeout=0.001",
            "dqlite://host:19001/db?timeout=3600",
        ],
    )
    def test_valid_range_accepted(self, url: str) -> None:
        dialect = DqliteDialect()
        _, kwargs = dialect.create_connect_args(make_url(url))
        assert kwargs


class TestURLMultiValueQueryParameter:
    """Repeated URL query keys resolve last-wins (a project convention matching
    Flask/Django, not an RFC mandate); the dialect cannot forward multiple values."""

    def test_repeated_timeout_takes_last_value(self) -> None:
        dialect = DqliteDialect()
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="t",
            query={"timeout": ("5", "10")},
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["timeout"] == 10.0

    def test_repeated_key_validator_runs_on_last_value(self) -> None:
        # Validator must run on the LAST value: tail "0" is out of range, so last-wins
        # raises; first-wins (raw[0]="1") would not.
        dialect = DqliteDialect()
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="t",
            query={"timeout": ("1", "0")},
        )
        with pytest.raises(ArgumentError, match="out of range"):
            dialect.create_connect_args(url)

    def test_repeated_bool_key_takes_last_value(self) -> None:
        # last-wins must hold for bool knobs too.
        dialect = DqliteDialect()
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="t",
            query={"trust_server_heartbeat": ("false", "true")},
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["trust_server_heartbeat"] is True


class TestURLGovernorsReachAioDbapi:
    """End-to-end: every URL governor knob must be accepted by the async DBAPI's
    connect() (the unit create_connect_args tests don't catch a kwarg-drop TypeError there)."""

    def test_all_governors_forwarded_end_to_end(self) -> None:
        from sqlalchemydqlite.aio import DqliteDialect_aio

        dialect = DqliteDialect_aio()
        url = make_url(
            "dqlite+aio://host:19001/db"
            "?timeout=5&max_total_rows=500"
            "&max_continuation_frames=7&trust_server_heartbeat=true"
        )
        _, kwargs = dialect.create_connect_args(url)
        aio_module = DqliteDialect_aio.import_dbapi()
        # A dropped or renamed knob would raise TypeError here.
        conn = aio_module.connect(**kwargs)
        assert conn.max_total_rows == 500
        assert kwargs == {
            "address": "host:19001",
            "database": "db",
            "timeout": 5.0,
            "max_total_rows": 500,
            "max_continuation_frames": 7,
            "trust_server_heartbeat": True,
        }


class TestDoPingNarrowExceptions:
    def test_returns_true_on_success(self) -> None:
        dialect = DqliteDialect()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.return_value = None
        assert dialect.do_ping(conn) is True

    def test_returns_false_on_operational_error(self) -> None:
        dialect = DqliteDialect()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError("bye")
        assert dialect.do_ping(conn) is False

    def test_propagates_unexpected_exception(self) -> None:
        dialect = DqliteDialect()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = RuntimeError("bug")
        with pytest.raises(RuntimeError, match="bug"):
            dialect.do_ping(conn)

    @pytest.mark.parametrize(
        "exc_cls,msg",
        [
            (AttributeError, "stale attribute"),
            (TypeError, "bad signature"),
            (AssertionError, "invariant violated"),
        ],
    )
    def test_propagates_programming_errors(self, exc_cls: type[Exception], msg: str) -> None:
        """A broadened except in do_ping would swallow refactor bugs and return False
        while pool health checks pass; pin propagation for each category."""
        dialect = DqliteDialect()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = exc_cls(msg)
        with pytest.raises(exc_cls, match=msg):
            dialect.do_ping(conn)


class TestIsDisconnectTypeDispatch:
    def test_dqlite_connection_error_is_disconnect(self) -> None:
        dialect = DqliteDialect()
        e = dqliteclient.exceptions.DqliteConnectionError("boom")
        assert dialect.is_disconnect(e, None, None) is True

    def test_os_error_is_disconnect(self) -> None:
        dialect = DqliteDialect()
        e = OSError(111, "Connection refused")
        assert dialect.is_disconnect(e, None, None) is True

    def test_broken_pipe_is_disconnect(self) -> None:
        dialect = DqliteDialect()
        assert dialect.is_disconnect(BrokenPipeError(), None, None) is True

    def test_leader_change_code_is_disconnect(self) -> None:
        dialect = DqliteDialect()
        e = dqliteclient.exceptions.OperationalError("not leader", 10250)
        assert dialect.is_disconnect(e, None, None) is True

    def test_unrelated_operational_error_is_not_disconnect(self) -> None:
        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("no such table")
        assert dialect.is_disconnect(e, None, None) is False

    def test_is_disconnect_true_for_every_leader_error_code(self) -> None:
        """Pin the full LEADER_ERROR_CODES tuple so dropping a member doesn't silently
        stop a real leadership transfer from triggering reconnect."""
        from dqlitewire import LEADER_ERROR_CODES

        dialect = DqliteDialect()
        for code in LEADER_ERROR_CODES:
            e = dqliteclient.exceptions.OperationalError("leader gone", code)
            assert dialect.is_disconnect(e, None, None) is True, (
                f"LEADER_ERROR_CODES member {code} must be classified "
                f"as a disconnect — is_disconnect returned False."
            )

    @pytest.mark.parametrize("code", [1, 5, 19, 14])
    def test_is_disconnect_false_for_non_leader_codes(self, code: int) -> None:
        """A stray OperationalError carrying a common SQLite code (BUSY, CONSTRAINT,
        CANTOPEN, generic) must not be misread as a disconnect."""
        dialect = DqliteDialect()
        e = dqliteclient.exceptions.OperationalError("application error", code)
        assert dialect.is_disconnect(e, None, None) is False

    @pytest.mark.parametrize(
        "exc_cls",
        [
            OSError,
            ConnectionError,
            BrokenPipeError,
            TimeoutError,
            ConnectionResetError,
            ConnectionAbortedError,
            ConnectionRefusedError,
            InterruptedError,
        ],
    )
    def test_every_oserror_subclass_is_disconnect(self, exc_cls: type[BaseException]) -> None:
        """Every stdlib OSError subclass classifies as a disconnect, guarding against a
        check narrowed to an explicit subclass enumeration."""
        dialect = DqliteDialect()
        assert dialect.is_disconnect(exc_cls("x"), None, None) is True

    def test_socket_gaierror_is_disconnect(self) -> None:
        """socket.gaierror (DNS-failure OSError subclass) classifies as a disconnect."""
        import socket

        dialect = DqliteDialect()
        assert dialect.is_disconnect(socket.gaierror("name or service"), None, None) is True


class TestSupportsSaneRowcountFlags:
    """Pin the supports_sane_rowcount flags on the dialect class itself so an upstream
    parent-default change can't silently alter dqlite behaviour."""

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("supports_sane_rowcount", True),
            ("supports_sane_multi_rowcount", True),
            ("supports_sane_rowcount_returning", False),
        ],
    )
    def test_flag_is_defined_on_dialect_class(self, attr: str, expected: bool) -> None:
        assert attr in vars(DqliteDialect), (
            f"{attr!r} must be defined on DqliteDialect, not merely inherited"
        )
        assert getattr(DqliteDialect, attr) is expected
