"""``do_ping`` must not swallow programming bugs from ``cursor.close``:
the close path narrows to connection-level errors (DEBUG-logged), letting
ValueError/TypeError from a refactor surface instead of disabling pre-ping.
"""

from __future__ import annotations

import logging

import pytest

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class _CursorExecuteOK:
    """execute succeeds; close is configurable per-test."""

    def __init__(self, on_close: Exception | None) -> None:
        self._on_close = on_close

    def execute(self, sql: str) -> None:
        pass

    def close(self) -> None:
        if self._on_close is not None:
            raise self._on_close


class _CursorExecuteRaises:
    """execute raises a connection-level error."""

    def __init__(self, on_execute: Exception, on_close: Exception | None) -> None:
        self._on_execute = on_execute
        self._on_close = on_close

    def execute(self, sql: str) -> None:
        raise self._on_execute

    def close(self) -> None:
        if self._on_close is not None:
            raise self._on_close


class _FakeConnection:
    """Minimal ``dbapi_connection`` — only ``cursor()`` is used."""

    def __init__(self, cursor: object) -> None:
        self._cursor = cursor

    def cursor(self) -> object:
        return self._cursor


def test_do_ping_propagates_programming_bug_from_close() -> None:
    """A ``ValueError`` from ``cursor.close`` must propagate."""
    cursor = _CursorExecuteOK(on_close=ValueError("refactor bug"))
    conn = _FakeConnection(cursor)
    dialect = DqliteDialect()
    with pytest.raises(ValueError, match="refactor bug"):
        dialect.do_ping(conn)


def test_do_ping_swallows_connection_level_close_errors_with_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A connection-level error on close is swallowed AND DEBUG-logged."""
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.base")
    cursor = _CursorExecuteRaises(
        on_execute=_dbapi_exc.OperationalError("dead"),
        on_close=_client_exc.DqliteConnectionError("socket closed"),
    )
    conn = _FakeConnection(cursor)
    dialect = DqliteDialect()
    assert dialect.do_ping(conn) is False
    messages = [r.getMessage() for r in caplog.records if r.name == "sqlalchemydqlite.base"]
    assert any("cursor.close failed" in m for m in messages), messages


def test_do_ping_happy_path_is_silent(caplog: pytest.LogCaptureFixture) -> None:
    """Successful ping: execute succeeds, close succeeds, no DEBUG noise."""
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.base")
    cursor = _CursorExecuteOK(on_close=None)
    conn = _FakeConnection(cursor)
    dialect = DqliteDialect()
    assert dialect.do_ping(conn) is True
    messages = [r.getMessage() for r in caplog.records if r.name == "sqlalchemydqlite.base"]
    assert not messages, f"happy path must not emit DEBUG logs; got {messages!r}"


@pytest.mark.parametrize(
    "close_exc",
    [
        _dbapi_exc.OperationalError("dead"),
        _dbapi_exc.InterfaceError("cursor is closed"),
        _client_exc.DqliteConnectionError("peer gone"),
        OSError(104, "connection reset"),
        BrokenPipeError(32, "broken pipe"),
        TimeoutError("read timed out"),
    ],
)
def test_do_ping_suppresses_close_path_transport_errors(
    caplog: pytest.LogCaptureFixture, close_exc: BaseException
) -> None:
    """After execute succeeds, a transport-error close is recoverable: ping
    returns True, DEBUG-logged. Pin every type in the suppression tuple."""
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.base")
    cursor = _CursorExecuteOK(on_close=close_exc)  # type: ignore[arg-type]
    conn = _FakeConnection(cursor)
    dialect = DqliteDialect()
    assert dialect.do_ping(conn) is True
    messages = [r.getMessage() for r in caplog.records if r.name == "sqlalchemydqlite.base"]
    assert any("cursor.close failed" in m for m in messages), messages
    assert any(type(close_exc).__name__ in m for m in messages), messages
