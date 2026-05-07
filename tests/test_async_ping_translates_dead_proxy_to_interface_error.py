"""Pin: ``DqliteDialect_aio._async_ping`` translates a dead
``weakref.proxy(_connection)`` (the post-close state of
``AsyncAdaptedConnection``) into a ``dbapi.InterfaceError`` rather
than letting a raw ``ReferenceError`` leak past SA's
``do_ping``-level ``dbapi.Error`` classifier.

``AsyncAdaptedConnection.close`` replaces ``self._connection`` with
``weakref.proxy(...)``. The wrapper's ``cursor()`` and
``run_async()`` already check for this and raise ``InterfaceError``
up front. ``_async_ping`` historically reached into
``dbapi_connection._connection.cursor()`` directly, bypassing the
guard. ``ReferenceError`` is not in SA's pre-ping classifier
(``OperationalError, ProgrammingError, InterfaceError,
DqliteConnectionError, OSError``) and would escape as an unhandled
programming bug.
"""

from __future__ import annotations

import weakref
from typing import Any, cast

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _DeadInnerProxyConnection:
    """Stand-in for an ``AsyncAdaptedConnection`` whose
    ``_connection`` slot has been replaced with a weakref proxy that
    points at a GC'd inner — the post-close state."""

    def __init__(self) -> None:
        # Build a dead proxy: target object goes away immediately.
        target = type("Dead", (), {})()
        proxy = weakref.proxy(target)
        del target  # proxy now points at a GC'd target
        self._connection = proxy

    def _handle_exception(self, error: BaseException) -> None:
        # Mirror AsyncAdaptedConnection's contract — surface as a
        # dbapi.Error subclass. Unused on the proxy-guard path because
        # the guard runs FIRST.
        raise error


@pytest.mark.asyncio
async def test_async_ping_dead_inner_proxy_raises_interface_error_not_reference_error() -> None:
    dialect = DqliteDialect_aio()
    fake_conn = cast(AsyncAdaptedConnection, _DeadInnerProxyConnection())

    from dqlitedbapi.exceptions import InterfaceError

    async def _runner() -> Any:
        try:
            await dialect._async_ping(fake_conn)
        except BaseException as e:
            return e
        return None

    captured = await greenlet_spawn(lambda: _runner())
    # The captured value is the awaitable; resolve it.
    result = await captured

    assert result is not None, "expected the ping to raise on a dead-proxy connection"
    assert isinstance(result, InterfaceError) or isinstance(result, ReferenceError) is False, (
        f"_async_ping must surface dbapi.InterfaceError on dead-proxy connection, "
        f"not {type(result).__name__}: {result}"
    )
    assert isinstance(result, InterfaceError), (
        f"got {type(result).__name__}: {result}; expected InterfaceError"
    )
