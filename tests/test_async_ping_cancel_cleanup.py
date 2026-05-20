"""Pin: ``DqliteDialect_aio._async_ping`` cancel-recovery contract.

`_async_ping` opens a raw dbapi cursor (NOT the SA adapter) and
runs ``execute`` / ``fetchone`` / ``close``. The close arm's
suppression scope was tightened to mirror
``AsyncAdaptedCursor.execute``'s sibling discipline:
``(Exception, asyncio.CancelledError)`` with a DEBUG record. This
file pins:

1. Cancel delivered at ``await cur.execute(...)`` propagates the
   ``CancelledError`` while the ``finally`` arm still runs
   ``cur.close()`` so the borrowed cursor does not leak.
2. A ``CancelledError`` raised from inside ``cur.close()`` is
   absorbed by the close arm and DEBUG-logged (the ping itself
   already succeeded; retiring the slot now would defeat
   pre-ping).
3. A plain ``RuntimeError`` from ``cur.close()`` is likewise
   absorbed.
4. A dbapi disconnect-class error (``OperationalError`` with a
   slot-fatal code) raised from close is no longer silent — the
   DEBUG record exposes it so a flapping leader is observable.
"""

from __future__ import annotations

import asyncio
import weakref
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _FakeInnerConnection:
    """A stand-in ``AsyncConnection`` returning a configurable cursor.

    Lets us inject ``execute`` / ``close`` coroutines that block,
    raise, or cancel mid-await without bringing up the full
    connection / event-loop machinery.
    """

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        # SA's ``is_disconnect`` proxy-check uses
        # ``weakref.ProxyTypes`` to detect a closed connection; this
        # stand-in is a real object so the guard at the top of
        # ``_async_ping`` does not short-circuit.

    def cursor(self) -> Any:
        return self._cursor


class _FakeAdaptedConnection:
    """Stand-in ``AsyncAdaptedConnection`` exposing only the surface
    that ``_async_ping`` reaches into.
    """

    def __init__(self, inner: _FakeInnerConnection) -> None:
        self._connection = inner

    def _handle_exception(self, error: BaseException) -> None:
        # Mirror the real adapter: rewrap RuntimeError as the dialect
        # expects. Tests below don't exercise this arm, but keep the
        # surface honest.
        raise error


@pytest.mark.asyncio
async def test_async_ping_cancel_mid_execute_runs_close_in_finally() -> None:
    """Cancel during ``await cur.execute(...)``: the cancel must
    propagate out, and the ``finally`` arm must still call
    ``cur.close()`` so the borrowed cursor does not leak.
    """
    close_called: list[bool] = []
    execute_started = asyncio.Event()

    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        execute_started.set()
        # Block forever — caller cancels the task.
        await asyncio.Event().wait()

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
    """If ``cur.close()`` raises ``CancelledError`` (e.g. a
    re-delivered cancel during the close coroutine), the close arm
    must absorb it — mirroring the sibling
    ``AsyncAdaptedCursor.execute`` close discipline — and emit a
    DEBUG record so the suppression is observable. The ping itself
    already succeeded, so retiring the slot now would defeat the
    point of pre-ping; ``KeyboardInterrupt`` / ``SystemExit`` still
    propagate because the catch tuple excludes them.
    """
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
        # Must NOT raise — the close-arm CancelledError is suppressed.
        await dialect._async_ping(adapted)

    assert close_called == [True], "cur.close() must run on the happy path"
    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "_async_ping cursor close" in msgs, (
        "the close-arm absorption must emit a DEBUG record (so a "
        "flapping leader is observable in logs)"
    )
    assert "CancelledError" in msgs


@pytest.mark.asyncio
async def test_async_ping_close_exception_during_normal_path_is_swallowed() -> None:
    """A plain ``RuntimeError`` from ``cur.close()`` on the happy
    path is absorbed by the ``(Exception, asyncio.CancelledError)``
    catch tuple so the ping returns success.
    """
    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        pass

    async def _fetchone() -> Any:
        return (1,)

    def _close() -> None:
        raise RuntimeError("close failed")

    cursor.execute = _execute
    cursor.fetchone = _fetchone
    cursor.close = _close

    adapted = cast(
        AsyncAdaptedConnection,
        _FakeAdaptedConnection(_FakeInnerConnection(cursor)),
    )

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    dialect._dialect_specific_select_one = "SELECT 1"

    # Must not raise — the close-arm RuntimeError is swallowed by
    # ``contextlib.suppress(Exception)``.
    await dialect._async_ping(adapted)


@pytest.mark.asyncio
async def test_async_ping_close_dbapi_error_is_logged_not_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A dbapi disconnect-class error (CORRUPT / FORMAT / NOTADB)
    raised by the close round-trip after a successful ping must be
    observable in DEBUG logs. The previous
    ``contextlib.suppress(Exception)`` silently absorbed these so a
    flapping leader was invisible at the ping site; this test pins
    the DEBUG emission so the suppression is no longer silent.
    """
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
    """Sanity: the dead-proxy guard at the top of ``_async_ping``
    still raises ``InterfaceError`` (covered by the sibling test,
    pinned here to ensure the cancel-arm tests above don't shadow
    the guard).
    """
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
