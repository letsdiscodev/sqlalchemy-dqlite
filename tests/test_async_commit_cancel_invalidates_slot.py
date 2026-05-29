"""A CancelledError fired during the COMMIT round-trip must propagate unchanged
through ``AsyncAdaptedConnection.commit`` so SA's pool-invalidation path runs.

Uses the real greenlet bridge (``greenlet_spawn``), not a ``_sync_await``
shortcut, since the shortcut short-circuits the cancellation propagation under
test."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _GatedAsyncConnection:
    """``commit`` parks on a test-controlled event so cancellation can fire
    deterministically mid-commit."""

    def __init__(self, started: asyncio.Event) -> None:
        self._started = started
        self.closed = False
        # The adapter logs ``self._connection.address`` on close failures.
        self.address = ("localhost", 0)

    async def commit(self) -> None:
        self._started.set()
        await asyncio.Event().wait()

    async def rollback(self) -> None: ...

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_cancellation_during_commit_propagates_unchanged() -> None:
    started = asyncio.Event()
    raw = _GatedAsyncConnection(started)
    adapter = AsyncAdaptedConnection(raw)

    async def driver() -> None:
        # SA invokes the sync ``adapter.commit`` from a greenlet; greenlet_spawn
        # provides the same bridge so ``await_only`` inside it works.
        await greenlet_spawn(adapter.commit)

    task = asyncio.create_task(driver())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cancellation_during_rollback_propagates_unchanged() -> None:
    """Symmetric assertion for ``rollback``, which shares ``commit``'s code path
    shape; pin the same contract to catch a future divergence."""

    class _GatedRollback:
        def __init__(self, started: asyncio.Event) -> None:
            self._started = started
            self.address = ("localhost", 0)

        async def commit(self) -> None: ...

        async def rollback(self) -> None:
            self._started.set()
            await asyncio.Event().wait()

        async def close(self) -> None: ...

    started = asyncio.Event()
    adapter = AsyncAdaptedConnection(_GatedRollback(started))

    async def driver() -> None:
        await greenlet_spawn(adapter.rollback)

    task = asyncio.create_task(driver())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
