"""Pin: ``AsyncAdaptedConnection.close()`` routes the
``RuntimeError("This event loop is already running")`` rollback failure
through ``_handle_exception`` so SA's ``is_disconnect`` (gated on
``DatabaseError``) classifies it.

Symmetric with the existing "different loop" remap in the close-rollback
arm and with ``_handle_exception``'s own ``"loop is already running"``
arm (``aio.py`` ~L1040). Without this arm a ``RuntimeError("This event
loop is already running")`` raised by ``await_only(rollback())`` during
``engine.dispose()`` would fall through the bare ``raise`` and propagate
past the dialect's ``has_terminate=True`` promise as an unclassified
RuntimeError.
"""

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
