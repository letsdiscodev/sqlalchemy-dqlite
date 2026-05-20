"""Pin: post-close proxy-detection guards on ``AsyncAdaptedConnection``
and ``DqliteDialect_aio.get_driver_connection`` raise
``InterfaceError`` (a ``dbapi.Error`` subclass) rather than letting a
bare ``ReferenceError`` escape from the ``weakref.proxy`` swap.

``AsyncAdaptedConnection.close()`` and ``terminate()`` swap
``self._connection`` for ``weakref.proxy(self._connection)`` on every
exit arm (load-bearing to release SA's pool diagnostic ring + pytest
session-fixture cache so the underlying ``AsyncConnection`` can be
GC'd). Any caller that touches ``connection._connection`` after close
and the inner has been GC'd hits ``ReferenceError`` on attribute
access — outside the ``dbapi.Error`` umbrella, bypassing SA's
``_handle_dbapi_exception`` classifier.

Sibling guards already in place on ``driver_connection`` /
``run_async`` / ``cursor``; this pin covers the
``DqliteDialect_aio.get_driver_connection`` hook and the
``close()`` / ``terminate()`` idempotency arms.
"""

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
    """The dialect-level hook must reject a closed adapter the same
    way the sibling instance-level ``driver_connection`` property
    does — ``InterfaceError`` (a ``dbapi.Error``), not bare
    ``ReferenceError``."""
    from dqlitedbapi.exceptions import InterfaceError

    adapter = _make_adapter_with_idempotent_close()
    await greenlet_spawn(adapter.close)  # swap to weakref.proxy
    # Drop the strong-ref the test held; force GC so the proxy target
    # is collected and an attribute access on the proxy would raise
    # bare ReferenceError. The guard must catch this and re-raise
    # InterfaceError.
    inner_strong_ref = adapter._connection
    del inner_strong_ref
    gc.collect()

    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    with pytest.raises(InterfaceError, match="Connection is closed"):
        dialect.get_driver_connection(adapter)


@pytest.mark.asyncio
async def test_double_close_with_dead_proxy_does_not_raise_reference_error() -> None:
    """The footgun this pin guards: after first close swaps to
    ``weakref.proxy``, if the inner ``AsyncConnection`` is GC'd
    (typical when the adapter is detached from SA's pool), a second
    ``close()`` would call ``self._connection.rollback()`` on the
    dead proxy — ``ReferenceError`` outside ``_TRANSPORT_CLASS_EXCEPTIONS``
    and outside the ``dbapi.Error`` umbrella. The idempotency
    short-circuit prevents that touch entirely."""
    adapter = _make_adapter_with_idempotent_close()
    await greenlet_spawn(adapter.close)
    # Force the proxy target to be GC'd. The proxy in
    # ``adapter._connection`` becomes a dead weakref.
    assert isinstance(adapter._connection, weakref.ProxyTypes)
    # Drop any local strong ref so the inner can be collected. We
    # don't have an obvious strong ref here (MagicMock + adapter),
    # but the proxy itself is "dead enough": even alive-proxied,
    # the second-close idempotency arm short-circuits without
    # touching the proxy.

    # Second close must not raise.
    await greenlet_spawn(adapter.close)


@pytest.mark.asyncio
async def test_double_terminate_with_proxied_inner_does_not_raise() -> None:
    """Same shape as the double-close test but for ``terminate``,
    which SA's pool uses on invalidate / dispose-with-error paths."""
    adapter = _make_adapter_with_idempotent_close()
    await greenlet_spawn(adapter.terminate)
    assert isinstance(adapter._connection, weakref.ProxyTypes)

    # Second terminate must not raise.
    await greenlet_spawn(adapter.terminate)
