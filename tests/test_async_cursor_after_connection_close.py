"""Pin the behaviour of an AsyncAdaptedCursor whose parent connection
has been closed.

The adapter intentionally does NOT track per-cursor handles for
cascade-close (mirrors SA's reference aiosqlite adapter) — each
execute/executemany opens a fresh dbapi cursor and closes it in a
finally block. So closing the adapter connection does not flip the
adapter cursor's _closed flag. Pin that the surface behaviour is
still correct: the next execute attempt surfaces the dbapi-layer
"Connection is closed" error from the underlying cursor() call,
not silent success.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import InterfaceError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def test_cursor_after_connection_close_raises_on_execute() -> None:
    underlying = MagicMock()
    # Simulate a closed underlying dbapi connection: cursor() raises
    # the same InterfaceError dqlitedbapi.aio.AsyncConnection raises
    # on use after close.
    underlying.cursor.side_effect = InterfaceError("Connection is closed")
    adapter_conn = AsyncAdaptedConnection(underlying)

    cursor = adapter_conn.cursor()
    assert isinstance(cursor, AsyncAdaptedCursor)
    # Simulate the adapter connection close having occurred — the
    # adapter's close path nulls / closes _connection on the
    # underlying side but leaves the SA cursor's _closed flag
    # unchanged. The next execute attempt must surface the underlying
    # InterfaceError, not silently succeed.
    with pytest.raises(InterfaceError, match="Connection is closed"):
        cursor.execute("SELECT 1")


def test_explicit_cursor_close_still_makes_execute_raise() -> None:
    """Independently of connection close: an explicitly closed adapter
    cursor must reject execute, per the closed-guard added for parity
    with the other adapter cursor methods."""
    adapter_conn = AsyncAdaptedConnection(MagicMock())
    cursor = adapter_conn.cursor()
    cursor.close()
    with pytest.raises(InterfaceError, match="cursor is closed"):
        cursor.execute("SELECT 1")
