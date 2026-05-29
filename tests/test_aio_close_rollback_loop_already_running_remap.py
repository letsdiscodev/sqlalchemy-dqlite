"""close() routes the rollback RuntimeError("This event loop is already
running") through _handle_exception so SA's is_disconnect classifies it."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


@pytest.mark.asyncio
async def test_close_remaps_loop_already_running_runtimeerror_to_operational_error() -> None:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError("This event loop is already running"))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    with pytest.raises(OperationalError, match="event loop already running"):
        await greenlet_spawn(adapter.close)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "msg",
    [
        "This event loop is ALREADY RUNNING",
        "LOOP IS ALREADY RUNNING",
        "this event loop is already running",
        "loop is Already Running",
    ],
)
async def test_close_remaps_loop_already_running_case_variants(msg: str) -> None:
    """The remap matches case-insensitively, so a CPython capitalisation
    tweak on this phrase must not bypass it."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError(msg))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    with pytest.raises(OperationalError, match="event loop already running"):
        await greenlet_spawn(adapter.close)
