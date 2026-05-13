"""Pin: ``DqliteDialect_aio._async_ping`` cancel-recovery contract.

`_async_ping` opens a raw dbapi cursor (NOT the SA adapter) and
runs ``execute`` / ``fetchone`` / ``close``. Three cancel-arms had
no fast-feedback test:

1. Cancel delivered at ``await cur.execute(...)`` MUST propagate
   ``CancelledError`` out (the ``finally`` arm's
   ``contextlib.suppress(Exception)`` does NOT cover
   ``CancelledError`` — that's deliberate).
2. The ``finally`` arm's ``await cur.close()`` MUST still run on
   the cancel path so the borrowed cursor does not leak.
3. A ``CancelledError`` re-fired from inside ``cur.close()`` must
   still propagate cleanly (no swallow), so SA's outer pool
   structured-concurrency invariants stay intact.

A regression that widened the inner ``suppress`` from
``Exception`` to ``BaseException`` would silently break (1) / (3).
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

    async def _close() -> None:
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
async def test_async_ping_cancel_re_raises_through_close_does_not_swallow() -> None:
    """If ``cur.close()`` itself raises ``CancelledError`` (e.g. a
    re-delivered cancel during the close coroutine), the outer
    ``contextlib.suppress(Exception)`` MUST NOT swallow it — the
    cancel must propagate to SA's outer pool path.
    """
    close_called: list[bool] = []
    execute_finished = asyncio.Event()

    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        execute_finished.set()

    async def _fetchone() -> Any:
        return (1,)

    async def _close() -> None:
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

    with pytest.raises(asyncio.CancelledError):
        await dialect._async_ping(adapted)

    assert close_called == [True], "cur.close() must run before cancel propagates"


@pytest.mark.asyncio
async def test_async_ping_close_exception_during_normal_path_is_swallowed() -> None:
    """The ``finally`` arm's ``contextlib.suppress`` IS scoped to
    ``Exception``: a plain ``RuntimeError`` from ``cur.close()`` on
    the happy path is absorbed so the ping returns success.
    """
    cursor = MagicMock()

    async def _execute(_sql: str) -> None:
        pass

    async def _fetchone() -> Any:
        return (1,)

    async def _close() -> None:
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
