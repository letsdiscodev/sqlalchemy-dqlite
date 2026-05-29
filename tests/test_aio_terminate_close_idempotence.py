"""``terminate()`` and ``close()`` are idempotent — calling either twice
(or one then the other) does not raise. SA's pool may call do_terminate
after a slot was already closed by an earlier failure cascade."""

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

    # Second terminate short-circuits on the proxy-state guard and never
    # reaches the inner — a GC'd proxy's close() would raise ReferenceError.
    assert adapter._connection.close.await_count == 1


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
