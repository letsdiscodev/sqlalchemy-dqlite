"""Pin: ``AsyncAdaptedConnection.close()`` and ``.terminate()`` run
the synchronous ``_force_close_transport`` fallback when an outer
``asyncio.CancelledError`` lands on the awaited inner work, and then
re-raise the cancel.

The same fallback shape covers ``MissingGreenlet`` (SA's pool
finalising a fairy from a non-greenlet context). This test closes
the parallel gap on the cancellation axis: SA's pool calls
``do_close`` or ``do_terminate`` from inside an outer
``asyncio.timeout()`` / ``asyncio.shield`` cancel during
``engine.dispose()``. Without the catch, the cancel propagates
before the sync fallback runs and the underlying writer transport
leaks (SA's pool absorbs the cancel as ``BaseException``).

The tests use ``greenlet_spawn`` so the inner ``await_only`` calls
are valid; the fake connection's awaited methods raise
``CancelledError`` to simulate the cancel landing on the await.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection

_FORCE_CLOSE_CALLS: list[str] = []


class _ProbedAdapter(AsyncAdaptedConnection):
    """Subclass overrides ``_force_close_transport`` to record
    invocations. The parent's ``__slots__`` prevents per-instance
    monkey-patching, so a subclass with a method override is the
    cleanest way to probe."""

    def _force_close_transport(self) -> None:
        _FORCE_CLOSE_CALLS.append("force_close")


def _adapter_with_force_close_probe() -> tuple[_ProbedAdapter, list[str]]:
    _FORCE_CLOSE_CALLS.clear()
    adapter = _ProbedAdapter.__new__(_ProbedAdapter)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.rollback = AsyncMock(side_effect=asyncio.CancelledError())
    inner.close = AsyncMock(side_effect=asyncio.CancelledError())
    adapter._connection = inner
    return adapter, _FORCE_CLOSE_CALLS


@pytest.mark.asyncio
async def test_close_force_closes_transport_on_cancel_then_propagates() -> None:
    adapter, calls = _adapter_with_force_close_probe()

    with pytest.raises(asyncio.CancelledError):
        await greenlet_spawn(adapter.close)

    assert calls == ["force_close"], (
        "close() must invoke _force_close_transport when CancelledError lands on the close await"
    )


@pytest.mark.asyncio
async def test_terminate_force_closes_transport_on_cancel_then_propagates() -> None:
    adapter, calls = _adapter_with_force_close_probe()

    with pytest.raises(asyncio.CancelledError):
        await greenlet_spawn(adapter.terminate)

    assert calls == ["force_close"]


@pytest.mark.asyncio
async def test_close_rollback_cancel_with_close_succeeding_propagates_cancel() -> None:
    """When rollback raises CancelledError and close succeeds, the
    CancelledError still propagates — preserving asyncio's
    "cancellation propagates" contract. ``force_close`` does NOT
    fire because the close ran cleanly. This is the contract pinned
    by the sibling test
    ``test_close_runs_close_after_rollback_raise.py``."""
    _FORCE_CLOSE_CALLS.clear()
    adapter = _ProbedAdapter.__new__(_ProbedAdapter)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.rollback = AsyncMock(side_effect=asyncio.CancelledError())
    inner.close = AsyncMock()  # succeeds
    adapter._connection = inner
    calls = _FORCE_CLOSE_CALLS

    with pytest.raises(asyncio.CancelledError):
        await greenlet_spawn(adapter.close)

    assert adapter._connection.close.await_count == 1
    assert calls == [], "force_close must not fire when the close itself completed cleanly"
