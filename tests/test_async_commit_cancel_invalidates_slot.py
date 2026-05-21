"""Pin: a CancelledError fired DURING the COMMIT round-trip
propagates unchanged through ``AsyncAdaptedConnection.commit`` so
SA's pool-invalidation path can run.

Sibling test ``test_async_cancel_inside_engine_begin.py`` covers
cancellation in the BODY of ``async with engine.begin():`` — that
path emits a ROLLBACK on ``__aexit__``. This test covers the
orthogonal path: cancellation fired during the COMMIT itself.

The contract: SA classifies ``CancelledError`` as
``is_exit_exception`` (sqlalchemy.util._concurrency_py3k:64),
which sets ``Connection._is_disconnect = True`` and triggers the
``finally`` clause's ``self.invalidate(e)`` in
``Connection._handle_dbapi_exception``. The pool slot is
discarded; the next acquire returns a fresh connection.

This test pins the inner-most precondition: ``CancelledError``
propagates UNCHANGED out of ``AsyncAdaptedConnection.commit``. If
``_handle_exception`` were to remap it to a non-``BaseException``
or swallow it, SA's invalidation chain would never fire. The full
SA-pool path is covered by the integration-side
``test_async_cancel_during_commit_invalidates_slot.py``.

Methodology note: this test uses the REAL greenlet bridge
(``greenlet_spawn``), NOT the ``_sync_await`` shortcut from
``test_async_executemany_leader_flip_state_reset.py``. The shortcut
monkey-patches ``await_only`` to a synchronous
``run_until_complete``, which short-circuits the very mechanism
under test (cancellation propagation through the greenlet bridge).
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _GatedAsyncConnection:
    """Stand-in for ``dqlitedbapi.aio.AsyncConnection`` shaped for
    the adapter. ``commit`` parks on a test-controlled event so the
    test can fire cancellation deterministically mid-commit."""

    def __init__(self, started: asyncio.Event) -> None:
        self._started = started
        self.closed = False
        # The adapter logs ``self._connection.address`` on close
        # failures; provide a safe stand-in.
        self.address = ("localhost", 0)

    async def commit(self) -> None:
        self._started.set()
        # Park forever; only cancellation breaks us out.
        await asyncio.Event().wait()

    async def rollback(self) -> None: ...

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_cancellation_during_commit_propagates_unchanged() -> None:
    """``CancelledError`` fired while ``AsyncAdaptedConnection.commit``
    is awaiting the underlying async connection's commit MUST
    propagate as ``CancelledError`` — not swallowed, not remapped to
    a regular ``Exception``. SA's pool-invalidation path depends on
    seeing the bare ``CancelledError`` in
    ``_handle_dbapi_exception``."""
    started = asyncio.Event()
    raw = _GatedAsyncConnection(started)
    adapter = AsyncAdaptedConnection(raw)

    async def driver() -> None:
        # ``adapter.commit`` is a sync method; SA invokes it from
        # inside a greenlet (the engine's transaction-commit path).
        # ``greenlet_spawn`` provides the same bridge so ``await_only``
        # inside ``adapter.commit`` works.
        await greenlet_spawn(adapter.commit)

    task = asyncio.create_task(driver())
    # Wait until the underlying commit is parked, then cancel.
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cancellation_during_rollback_propagates_unchanged() -> None:
    """Symmetric assertion for ``rollback``: ``CancelledError`` fired
    during the rollback await also propagates unchanged. ``rollback``
    is on the same code path shape as ``commit`` in
    ``AsyncAdaptedConnection`` (lines 489-493 of aio.py); pin the
    same contract so a future divergence between the two methods is
    caught."""

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
