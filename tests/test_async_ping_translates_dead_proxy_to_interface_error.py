"""Pin: ``DqliteDialect_aio._async_ping`` translates a dead post-close
``weakref.proxy(_connection)`` into ``dbapi.InterfaceError`` — a raw
``ReferenceError`` is not in SA's pre-ping classifier and would leak."""

from __future__ import annotations

import weakref
from typing import Any, cast

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _DeadInnerProxyConnection:
    """Post-close ``AsyncAdaptedConnection``: ``_connection`` is a proxy to a
    GC'd inner."""

    def __init__(self) -> None:
        target = type("Dead", (), {})()
        proxy = weakref.proxy(target)
        del target  # proxy now points at a GC'd target
        self._connection = proxy

    def _handle_exception(self, error: BaseException) -> None:
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
    result = await captured

    assert result is not None, "expected the ping to raise on a dead-proxy connection"
    assert isinstance(result, InterfaceError) or isinstance(result, ReferenceError) is False, (
        f"_async_ping must surface dbapi.InterfaceError on dead-proxy connection, "
        f"not {type(result).__name__}: {result}"
    )
    assert isinstance(result, InterfaceError), (
        f"got {type(result).__name__}: {result}; expected InterfaceError"
    )
