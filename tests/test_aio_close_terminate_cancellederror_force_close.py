"""close() and terminate() run the sync _force_close_transport fallback when an
outer CancelledError lands on the awaited inner work, then re-raise the cancel —
otherwise the writer transport leaks (SA's pool absorbs the cancel)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection

_FORCE_CLOSE_CALLS: list[str] = []


class _ProbedAdapter(AsyncAdaptedConnection):
    """Records _force_close_transport calls; subclassed because the parent's
    __slots__ blocks per-instance monkey-patching."""

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
    """Rollback CancelledError + clean close: the cancel still propagates and
    force_close does NOT fire (close ran cleanly)."""
    _FORCE_CLOSE_CALLS.clear()
    adapter = _ProbedAdapter.__new__(_ProbedAdapter)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.rollback = AsyncMock(side_effect=asyncio.CancelledError())
    inner.close = AsyncMock()
    adapter._connection = inner
    calls = _FORCE_CLOSE_CALLS

    with pytest.raises(asyncio.CancelledError):
        await greenlet_spawn(adapter.close)

    assert adapter._connection.close.await_count == 1
    assert calls == [], "force_close must not fire when the close itself completed cleanly"
