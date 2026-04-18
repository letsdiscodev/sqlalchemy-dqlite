"""Dialect configuration tests covering recent hardening.

- ISSUE-16 ``do_ping`` narrowed to connection-level exceptions only.
- ISSUE-18 ``is_disconnect`` type-dispatches before falling back to
  message substring matching.
- ISSUE-19 ``create_connect_args`` plumbs the ``timeout`` URL query
  through and rejects typos.
- ISSUE-21 ``set_isolation_level`` explicitly rejects AUTOCOMMIT.
- ISSUE-23 ``supported_isolation_levels`` is declared.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions
import dqlitedbapi.exceptions
from sqlalchemydqlite.base import DqliteDialect


class TestSupportedIsolationLevels:
    def test_declared(self) -> None:
        assert DqliteDialect.supported_isolation_levels == ("SERIALIZABLE",)


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
