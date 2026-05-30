"""Post-close proxy guards raise InterfaceError (a dbapi.Error) rather
than let a bare ReferenceError escape the ``weakref.proxy`` swap that
close()/terminate() install. A bare ReferenceError is outside the
dbapi.Error umbrella and bypasses SA's _handle_dbapi_exception
classifier. Covers get_driver_connection and the close/terminate
idempotency arms."""

from __future__ import annotations

import gc
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import (
    AsyncAdaptedConnection,
    DqliteDialect_aio,
)


def _make_adapter_with_idempotent_close() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.rollback = AsyncMock()
    inner.close = AsyncMock()
    adapter._connection = inner
    return adapter


@pytest.mark.asyncio
async def test_get_driver_connection_after_close_raises_interface_error() -> None:
    """The dialect-level hook must reject a closed adapter with
    InterfaceError, not bare ReferenceError."""
    from dqlitedbapi.exceptions import InterfaceError

    adapter = _make_adapter_with_idempotent_close()
    await greenlet_spawn(adapter.close)  # swap to weakref.proxy
    # Force GC of the proxy target so an attribute access would raise
    # bare ReferenceError; the guard must re-raise InterfaceError.
    inner_strong_ref = adapter._connection
    del inner_strong_ref
    gc.collect()

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    with pytest.raises(InterfaceError, match="Connection is closed"):
        dialect.get_driver_connection(adapter)


@pytest.mark.asyncio
async def test_double_close_with_dead_proxy_does_not_raise_reference_error() -> None:
    """After the first close swaps to proxy and the inner is GC'd, a
    second close() would call rollback() on the dead proxy and raise
    ReferenceError; the idempotency short-circuit prevents that touch."""
    adapter = _make_adapter_with_idempotent_close()
    await greenlet_spawn(adapter.close)
    assert isinstance(adapter._connection, weakref.ProxyTypes)
    # Even alive-proxied, the second-close idempotency arm short-circuits
    # without touching the proxy.
    await greenlet_spawn(adapter.close)


@pytest.mark.asyncio
async def test_double_terminate_with_proxied_inner_does_not_raise() -> None:
    """Same as the double-close test but for ``terminate``, used by SA's
    pool on invalidate / dispose-with-error paths."""
    adapter = _make_adapter_with_idempotent_close()
    await greenlet_spawn(adapter.terminate)
    assert isinstance(adapter._connection, weakref.ProxyTypes)
    await greenlet_spawn(adapter.terminate)


class _CallableTargetForProxy:
    """Wrapped as CallableProxyType: the callable variant reliably makes
    ``isinstance(dead_proxy, ProxyTypes)`` raise ReferenceError across
    versions (non-callable ProxyType has a CPython 3.13+ fast-path)."""

    def __call__(self) -> None:
        return None


def test_isinstance_on_dead_callable_proxy_raises_reference_error() -> None:
    """Why the guards use ``type(x) in ProxyTypes`` not ``isinstance``:
    isinstance on a dead CallableProxyType forwards through __class__ and
    raises ReferenceError."""
    target = _CallableTargetForProxy()
    proxy = weakref.proxy(target)
    assert isinstance(proxy, weakref.CallableProxyType)
    del target
    gc.collect()

    # The buggy form raises; the correct form does not.
    with pytest.raises(ReferenceError):
        isinstance(proxy, weakref.ProxyTypes)
    assert type(proxy) in weakref.ProxyTypes
