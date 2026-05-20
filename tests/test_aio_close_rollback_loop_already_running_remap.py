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
    """The remap uses ``msg_lower.find("loop is already running")`` so a
    CPython capitalisation tweak on the third RuntimeError phrase must
    NOT silently bypass the remap. Mirror of the case-insensitive pins
    already in place for the other two substrings (``"different loop"``
    and ``"event loop is closed"``). Without this coverage the third
    arm could regress to canonical-case-only and ``engine.dispose()``
    would propagate a bare RuntimeError past the
    ``has_terminate=True`` contract."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError(msg))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    with pytest.raises(OperationalError, match="event loop already running"):
        await greenlet_spawn(adapter.close)
