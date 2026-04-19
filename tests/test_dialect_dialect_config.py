"""Dialect configuration tests covering recent hardening.

- ``do_ping`` narrowed to connection-level exceptions only.
- ``is_disconnect`` type-dispatches before falling back to
  message substring matching.
- ``create_connect_args`` plumbs the ``timeout`` URL query
  through and rejects typos.
- ``set_isolation_level`` explicitly rejects AUTOCOMMIT.
- ``get_isolation_level_values`` advertises only SERIALIZABLE.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions
import dqlitedbapi.exceptions
from sqlalchemydqlite.base import DqliteDialect


class TestGetIsolationLevelValues:
    def test_only_serializable(self) -> None:
        """dqlite accepts SERIALIZABLE only; inheriting SQLiteDialect's
        ``["READ UNCOMMITTED", "SERIALIZABLE"]`` would let callers set a
        level we cannot honour (PRAGMA read_uncommitted has no effect
        server-side).
        """
        dialect = DqliteDialect()
        assert dialect.get_isolation_level_values(MagicMock()) == ["SERIALIZABLE"]

    def test_read_uncommitted_not_advertised(self) -> None:
        dialect = DqliteDialect()
        assert "READ UNCOMMITTED" not in dialect.get_isolation_level_values(MagicMock())

    def test_defined_locally(self) -> None:
        """Must be overridden on DqliteDialect itself; inheriting
        SQLiteDialect's version would silently re-introduce
        READ UNCOMMITTED.
        """
        assert "get_isolation_level_values" in DqliteDialect.__dict__


class TestSetIsolationLevel:
    def test_serializable_is_noop(self) -> None:
        dialect = DqliteDialect()
        dialect.set_isolation_level(MagicMock(), "SERIALIZABLE")  # no raise

    def test_autocommit_rejected(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            dialect.set_isolation_level(MagicMock(), "AUTOCOMMIT")

    def test_other_levels_warn(self) -> None:
        dialect = DqliteDialect()
        with pytest.warns(UserWarning, match="SERIALIZABLE"):
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
        """max_total_rows URL param plumbs through to the DBAPI."""
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=5000")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] == 5000

    def test_max_total_rows_unparseable_raises(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=not-an-int")
        with pytest.raises(ArgumentError, match="int"):
            dialect.create_connect_args(url)

    def test_timeout_and_max_total_rows_together(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?timeout=3.5&max_total_rows=250")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["timeout"] == 3.5
        assert kwargs["max_total_rows"] == 250

    def test_max_continuation_frames_forwarded(self) -> None:
        """max_continuation_frames URL plumbing — post-review follow-up."""
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
        """trust_server_heartbeat URL plumbing — post-review follow-up.

        URL values arrive as strings; bool("False") would evaluate
        truthy if used directly, so we use a dedicated parser.
        """
        dialect = DqliteDialect()
        url = make_url(f"dqlite://host:19001/db?trust_server_heartbeat={raw}")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["trust_server_heartbeat"] is expected

    @pytest.mark.parametrize(
        "raw",
        ["enabled", "flase", "yse", "2", "maybe"],
    )
    def test_trust_server_heartbeat_rejects_unknown_tokens(self, raw: str) -> None:
        """Unknown tokens must raise rather than silently coerce to False.

        A typo in the URL would previously mean the operator thinks they
        opted into the flag but actually got the default — a surprising
        and hard-to-diagnose misconfiguration.
        """
        dialect = DqliteDialect()
        url = make_url(f"dqlite://host:19001/db?trust_server_heartbeat={raw}")
        with pytest.raises(ArgumentError, match="Invalid bool value"):
            dialect.create_connect_args(url)


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
        # Just smoke-test that parsing completed without error.
        assert kwargs


class TestURLGovernorsReachAioDbapi:
    """End-to-end test: every URL governor knob must be accepted by the
    async DBAPI's connect(). The unit-level create_connect_args tests only
    prove the dialect builds the right kwargs dict — they don't catch the
    regression where aio.connect() silently drops kwargs and raises
    TypeError when invoked by DqliteDialect_aio.connect().
    """

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
        # Must not raise TypeError: unexpected keyword argument.
        conn = aio_module.connect(**kwargs)
        assert conn._max_total_rows == 500
        assert conn._max_continuation_frames == 7
        assert conn._trust_server_heartbeat is True
        assert conn._timeout == 5


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
        """A broadened except in do_ping would swallow refactor bugs
        (AttributeError from a renamed attribute, TypeError from a
        stale signature, AssertionError from a violated invariant) and
        silently return False — pool health checks would pass while
        the real fault rotted. Pin propagation for each category."""
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
        e = dqliteclient.exceptions.OperationalError(10250, "not leader")
        assert dialect.is_disconnect(e, None, None) is True

    def test_unrelated_operational_error_is_not_disconnect(self) -> None:
        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("no such table")
        assert dialect.is_disconnect(e, None, None) is False

    def test_is_disconnect_true_for_every_leader_error_code(self) -> None:
        """Pin the full LEADER_ERROR_CODES tuple — if a future refactor
        drops SQLITE_IOERR_LEADERSHIP_LOST from the constant, a real
        leadership transfer stops triggering the reconnect path and the
        only signal is application latency, not a test failure."""
        from dqlitewire import LEADER_ERROR_CODES

        dialect = DqliteDialect()
        for code in LEADER_ERROR_CODES:
            e = dqliteclient.exceptions.OperationalError(code, "leader gone")
            assert dialect.is_disconnect(e, None, None) is True, (
                f"LEADER_ERROR_CODES member {code} must be classified "
                f"as a disconnect — is_disconnect returned False."
            )

    @pytest.mark.parametrize("code", [1, 5, 19, 14])
    def test_is_disconnect_false_for_non_leader_codes(self, code: int) -> None:
        """Defensive fence against future reclassification drift: a stray
        OperationalError carrying a common SQLite code (generic error,
        SQLITE_BUSY, SQLITE_CONSTRAINT, SQLITE_CANTOPEN) must not be
        misread as a disconnect. Constraint codes in particular now
        surface as IntegrityError after ISSUE-209, but this test pins
        the fallback behaviour for any stray OperationalError that
        slips through."""
        dialect = DqliteDialect()
        e = dqliteclient.exceptions.OperationalError(code, "application error")
        assert dialect.is_disconnect(e, None, None) is False
