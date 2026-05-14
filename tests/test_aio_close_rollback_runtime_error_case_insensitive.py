"""Pin: ``AsyncAdaptedConnection.close()``'s inner-rollback
``RuntimeError`` arm matches the loop-mismatch / event-loop-closed
substrings case-insensitively, mirroring the sibling
``_handle_exception`` ``.lower()`` discipline and the
``is_disconnect`` substring scan in ``base.py``.

The canonical phrases shipped by CPython today are
``"attached to a different loop"`` and ``"Event loop is closed"``,
but the CPython source-of-truth for both is implementation-level
(asyncio internals), not stable documented API. A future capital-
isation tweak between point releases would silently bypass the
remap and let a bare ``RuntimeError`` propagate past
``engine.dispose()``, breaking SA's ``has_terminate=True`` contract.

The disconnect classifier in ``base.py::is_disconnect`` already
lowercases its input (post-d8ecb49); ``_handle_exception`` does the
same at ``aio.py:1004``. The inner-rollback arm at ``aio.py:1140-1153``
was the asymmetric raw-case site; this test pins it under uppercase
variants of both substrings.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


@pytest.mark.asyncio
async def test_close_rollback_runtime_error_different_loop_uppercase_remapped() -> None:
    """RuntimeError with uppercase ``"DIFFERENT LOOP"`` raised by the
    rollback await must still route through ``_handle_exception``
    (which raises OperationalError)."""
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
    """RuntimeError with uppercase ``"EVENT LOOP IS CLOSED"`` raised
    by the rollback await must be suppressed (debug-log + return),
    not propagated. ``has_terminate=True`` requires dispose to stay
    quiet on this arm. The outer close runs via the finally block
    after the suppression returns."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.rollback = AsyncMock(side_effect=RuntimeError("EVENT LOOP IS CLOSED"))
    inner.close = AsyncMock()
    inner.address = "localhost:9001"
    adapter._connection = inner

    # Must not raise.
    await greenlet_spawn(adapter.close)
