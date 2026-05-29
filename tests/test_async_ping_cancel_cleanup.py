"""Pin: ``DqliteDialect_aio._async_ping`` cancel-recovery contract — the
``finally`` arm always closes the borrowed cursor, the close arm absorbs
cancel/transport errors with a DEBUG record (the ping already succeeded),
and dbapi disconnect-class errors from close are logged not silent."""

from __future__ import annotations

import asyncio
import weakref
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _FakeInnerConnection:
    """Stand-in ``AsyncConnection`` returning a configurable cursor."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        # A real object (not a dead proxy), so _async_ping's top-of-method
        # proxy guard does not short-circuit.

    def cursor(self) -> Any:
        return self._cursor


class _FakeAdaptedConnection:
    """Stand-in ``AsyncAdaptedConnection`` exposing only what ``_async_ping`` uses."""

    def __init__(self, inner: _FakeInnerConnection) -> None:
        self._connection = inner

    def _handle_exception(self, error: BaseException) -> None:
        raise error


@pytest.mark.asyncio
async def test_async_ping_cancel_mid_execute_runs_close_in_finally() -> None:
    """Cancel during ``await cur.execute(...)`` propagates, but ``finally``
    still calls ``cur.close()`` so the borrowed cursor does not leak."""
    close_called: list[bool] = []
    execute_started = asyncio.Event()

    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        execute_started.set()
        await asyncio.Event().wait()  # block forever; caller cancels

    def _close() -> None:
        close_called.append(True)

    cursor.execute = _execute
    cursor.fetchone = MagicMock()
    cursor.close = _close

    adapted = cast(
        AsyncAdaptedConnection,
        _FakeAdaptedConnection(_FakeInnerConnection(cursor)),
    )

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    dialect._dialect_specific_select_one = "SELECT 1"

    task = asyncio.create_task(dialect._async_ping(adapted))
    await execute_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert close_called == [True], (
        "the finally arm of _async_ping must call cur.close() even when "
        "the await is cancelled mid-execute"
    )


@pytest.mark.asyncio
async def test_async_ping_close_cancellederror_absorbed_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``CancelledError`` from ``cur.close()`` is absorbed and DEBUG-logged;
    the ping already succeeded, so retiring the slot would defeat pre-ping."""
    close_called: list[bool] = []

    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        pass

    async def _fetchone() -> Any:
        return (1,)

    def _close() -> None:
        close_called.append(True)
        raise asyncio.CancelledError

    cursor.execute = _execute
    cursor.fetchone = _fetchone
    cursor.close = _close

    adapted = cast(
        AsyncAdaptedConnection,
        _FakeAdaptedConnection(_FakeInnerConnection(cursor)),
    )

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    dialect._dialect_specific_select_one = "SELECT 1"

    import logging

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        await dialect._async_ping(adapted)  # must not raise

    assert close_called == [True], "cur.close() must run on the happy path"
    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "_async_ping cursor close" in msgs, (
        "the close-arm absorption must emit a DEBUG record (so a "
        "flapping leader is observable in logs)"
    )
    assert "CancelledError" in msgs


@pytest.mark.asyncio
async def test_async_ping_close_transport_class_exception_is_swallowed() -> None:
    """A transport-class exception (here ``OSError``) from ``cur.close()`` is
    absorbed by the narrow catch tuple so the ping returns success."""
    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        pass

    async def _fetchone() -> Any:
        return (1,)

    def _close() -> None:
        raise OSError("close failed: simulated transport teardown")

    cursor.execute = _execute
    cursor.fetchone = _fetchone
    cursor.close = _close

    adapted = cast(
        AsyncAdaptedConnection,
        _FakeAdaptedConnection(_FakeInnerConnection(cursor)),
    )

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    dialect._dialect_specific_select_one = "SELECT 1"

    await dialect._async_ping(adapted)  # must not raise; OSError is in the narrow tuple


@pytest.mark.asyncio
async def test_async_ping_close_dbapi_error_is_logged_not_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A dbapi disconnect-class error from close after a successful ping must
    be observable in DEBUG logs, not silently absorbed (flapping leader)."""
    from dqlitedbapi.exceptions import OperationalError

    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        pass

    async def _fetchone() -> Any:
        return (1,)

    def _close() -> None:
        raise OperationalError("CORRUPT: leader flip mid-close", code=11)

    cursor.execute = _execute
    cursor.fetchone = _fetchone
    cursor.close = _close

    adapted = cast(
        AsyncAdaptedConnection,
        _FakeAdaptedConnection(_FakeInnerConnection(cursor)),
    )

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    dialect._dialect_specific_select_one = "SELECT 1"

    import logging

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        await dialect._async_ping(adapted)

    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "_async_ping cursor close" in msgs, "dbapi error from close must surface in DEBUG logs"
    assert "OperationalError" in msgs


@pytest.mark.asyncio
async def test_async_ping_dead_proxy_guard_unrelated_sanity() -> None:
    """Sanity: the dead-proxy guard still raises ``InterfaceError`` (so the
    cancel-arm tests above don't shadow it)."""
    target = type("Dead", (), {})()
    proxy = weakref.proxy(target)
    del target

    class _Wrapper:
        def __init__(self) -> None:
            self._connection = proxy

        def _handle_exception(self, error: BaseException) -> None:
            raise error

    from dqlitedbapi.exceptions import InterfaceError

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    dialect._dialect_specific_select_one = "SELECT 1"

    with pytest.raises(InterfaceError):
        await dialect._async_ping(cast(AsyncAdaptedConnection, _Wrapper()))
