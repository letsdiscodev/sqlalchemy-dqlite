"""Pin: ``terminate()`` and ``close()`` on ``AsyncAdaptedConnection``
are idempotent — calling either twice (or one then the other) does
not raise.

SA's async pool may call ``do_terminate`` after a slot has already
been closed by an earlier failure path (e.g., ``engine.dispose()``
during a cascade where one connection's close raised and triggered
forced reclaim of every checked-out slot). The dbapi
``AsyncConnection.close`` is documented as idempotent; pin that the
adapter preserves that property for both calls.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter_with_idempotent_close() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.rollback = AsyncMock()
    inner.close = AsyncMock()
    adapter._connection = inner
    return adapter


@pytest.mark.asyncio
async def test_terminate_twice_does_not_raise() -> None:
    adapter = _make_adapter_with_idempotent_close()

    await greenlet_spawn(adapter.terminate)
    await greenlet_spawn(adapter.terminate)

    # Both calls reached the underlying close; the inner mock is
    # idempotent (every AsyncMock() invocation returns its return
    # value without raising). The pin is "no exception escapes".
    assert adapter._connection.close.await_count == 2


@pytest.mark.asyncio
async def test_close_then_terminate_does_not_raise() -> None:
    adapter = _make_adapter_with_idempotent_close()

    await greenlet_spawn(adapter.close)
    await greenlet_spawn(adapter.terminate)


@pytest.mark.asyncio
async def test_terminate_then_close_does_not_raise() -> None:
    adapter = _make_adapter_with_idempotent_close()

    await greenlet_spawn(adapter.terminate)
    await greenlet_spawn(adapter.close)


@pytest.mark.asyncio
async def test_close_twice_does_not_raise() -> None:
    adapter = _make_adapter_with_idempotent_close()

    await greenlet_spawn(adapter.close)
    await greenlet_spawn(adapter.close)
