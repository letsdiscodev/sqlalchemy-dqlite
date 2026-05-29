"""Pin: ``AsyncAdaptedCursor`` routes errors from both the synchronous ``cursor()`` call and
the ``execute``/``executemany`` await through ``_handle_exception``."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import InterfaceError, OperationalError, ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _build_cursor_with_inner(inner: MagicMock) -> AsyncAdaptedCursor:
    adapt_conn = MagicMock()
    adapt_conn._connection = inner

    def handle(exc: BaseException) -> None:
        if isinstance(exc, RuntimeError) and "different loop" in str(exc):
            raise OperationalError(f"event-loop mismatch: {exc}", code=None) from exc
        if isinstance(exc, ProgrammingError) and "different event loop" in str(exc):
            raise OperationalError(f"event-loop mismatch: {exc}", code=None) from exc
        raise exc

    adapt_conn._handle_exception = handle
    return AsyncAdaptedCursor(adapt_conn)


def test_cursor_time_interface_error_propagates_unchanged() -> None:
    """A synchronous InterfaceError from cursor() re-raises with its class preserved."""
    inner = MagicMock()
    inner.cursor.side_effect = InterfaceError("Connection is closed")
    cursor = _build_cursor_with_inner(inner)

    with pytest.raises(InterfaceError, match="closed"):
        cursor.execute("SELECT 1")


def test_cursor_time_loop_mismatch_programmingerror_remapped() -> None:
    """A synchronous cross-loop ProgrammingError from cursor() is remapped to OperationalError."""
    inner = MagicMock()
    inner.cursor.side_effect = ProgrammingError("AsyncConnection used from a different event loop")
    cursor = _build_cursor_with_inner(inner)

    with pytest.raises(OperationalError, match="event-loop mismatch"):
        cursor.execute("SELECT 1")


def test_execute_time_cancellederror_propagates() -> None:
    """A CancelledError from execute propagates unchanged; cursor state is cleared."""
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
