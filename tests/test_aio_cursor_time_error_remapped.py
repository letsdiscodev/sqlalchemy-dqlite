"""Pin: ``AsyncAdaptedCursor`` routes errors from BOTH the
synchronous ``cursor()`` call and the ``cursor.execute`` /
``cursor.executemany`` await through ``_handle_exception``.

The cursor()-time hoist (see commit "Hoist cursor() inside try and
remap loop-mismatch ProgrammingError") moved the cursor-construction
inside the try so synchronous raises (closed connection,
cross-loop ProgrammingError) get the same remap treatment as
cursor.execute raises. This file pins both paths.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import InterfaceError, OperationalError, ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _build_cursor_with_inner(inner: MagicMock) -> AsyncAdaptedCursor:
    """Build an AsyncAdaptedCursor with a stub adapter and inner conn."""
    adapt_conn = MagicMock()
    adapt_conn._connection = inner

    # Real _handle_exception wires up the same way the live class does.
    def handle(exc: BaseException) -> None:
        if isinstance(exc, RuntimeError) and "different loop" in str(exc):
            raise OperationalError(f"event-loop mismatch: {exc}", code=None) from exc
        if isinstance(exc, ProgrammingError) and "different event loop" in str(exc):
            raise OperationalError(f"event-loop mismatch: {exc}", code=None) from exc
        raise exc

    adapt_conn._handle_exception = handle
    return AsyncAdaptedCursor(adapt_conn)


def test_cursor_time_interface_error_propagates_unchanged() -> None:
    """A synchronous InterfaceError from cursor() routes through
    ``_handle_exception`` (no remap, just re-raise) — pin that the
    error class is preserved."""
    inner = MagicMock()
    inner.cursor.side_effect = InterfaceError("Connection is closed")
    cursor = _build_cursor_with_inner(inner)

    with pytest.raises(InterfaceError, match="closed"):
        cursor.execute("SELECT 1")


def test_cursor_time_loop_mismatch_programmingerror_remapped() -> None:
    """A synchronous ProgrammingError("...different event loop...")
    from cursor() routes through ``_handle_exception`` and is
    remapped to OperationalError so SA's is_disconnect substring
    branch picks it up."""
    inner = MagicMock()
    inner.cursor.side_effect = ProgrammingError("AsyncConnection used from a different event loop")
    cursor = _build_cursor_with_inner(inner)

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        cursor.execute("SELECT 1")


def test_execute_time_cancellederror_propagates() -> None:
    """A CancelledError from cursor.execute propagates unchanged
    through ``_handle_exception``. State is cleared as a side
    effect of the cancel-or-error path."""
    inner = MagicMock()
    inner_cursor = MagicMock()
    inner_cursor.execute.side_effect = asyncio.CancelledError("greenlet cancel")
    inner.cursor.return_value = inner_cursor
    cursor = _build_cursor_with_inner(inner)

    with pytest.raises(asyncio.CancelledError):
        cursor.execute("SELECT 1")
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
    assert list(cursor._rows) == []
