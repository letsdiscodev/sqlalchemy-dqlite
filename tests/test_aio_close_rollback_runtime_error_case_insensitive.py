"""close()'s inner-rollback RuntimeError arm matches the loop-mismatch /
event-loop-closed substrings case-insensitively: the phrases are asyncio
internals, not stable API, so a capitalisation tweak must not bypass it."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


@pytest.mark.asyncio
async def test_close_rollback_runtime_error_different_loop_uppercase_remapped() -> None:
    """Uppercase "DIFFERENT LOOP" still routes through _handle_exception."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError("Future ... attached to a DIFFERENT LOOP"))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        await greenlet_spawn(adapter.close)


@pytest.mark.asyncio
async def test_close_rollback_runtime_error_event_loop_closed_uppercase_suppressed() -> None:
    """Uppercase "EVENT LOOP IS CLOSED" must be suppressed (debug-log), not
    propagated, so dispose stays quiet per has_terminate=True."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError("EVENT LOOP IS CLOSED"))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    await greenlet_spawn(adapter.close)  # must not raise
