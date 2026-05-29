"""An AsyncAdaptedCursor whose parent connection was closed must surface the
dbapi-layer "Connection is closed" error on the next execute, not succeed.

The adapter does not track per-cursor handles for cascade-close (mirrors SA's
aiosqlite adapter), so connection close does not flip the cursor's _closed flag."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import InterfaceError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def test_cursor_after_connection_close_raises_on_execute() -> None:
    underlying = MagicMock()
    # Closed underlying connection: cursor() raises the same InterfaceError
    # dqlitedbapi.aio.AsyncConnection raises on use after close.
    underlying.cursor.side_effect = InterfaceError("Connection is closed")
    adapter_conn = AsyncAdaptedConnection(underlying)

    cursor = adapter_conn.cursor()
    assert isinstance(cursor, AsyncAdaptedCursor)
    with pytest.raises(InterfaceError, match="Connection is closed"):
        cursor.execute("SELECT 1")


def test_explicit_cursor_close_still_makes_execute_raise() -> None:
    """An explicitly closed adapter cursor rejects execute, independent of
    connection close."""
    adapter_conn = AsyncAdaptedConnection(MagicMock())
    cursor = adapter_conn.cursor()
    cursor.close()
    with pytest.raises(InterfaceError, match="cursor is closed"):
        cursor.execute("SELECT 1")
