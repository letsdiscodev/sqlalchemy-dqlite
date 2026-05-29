"""close() routes the loop-mismatch RuntimeError through _handle_exception
so SA's is_disconnect (gated on DatabaseError) can classify it."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


@pytest.mark.asyncio
async def test_close_remaps_loop_mismatch_runtimeerror_to_operational_error() -> None:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError("Future ... attached to a different loop"))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        await greenlet_spawn(adapter.close)


@pytest.mark.asyncio
async def test_close_propagates_other_runtimeerrors_unchanged() -> None:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError("some other failure"))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    with pytest.raises(RuntimeError, match="some other failure"):
        await greenlet_spawn(adapter.close)
