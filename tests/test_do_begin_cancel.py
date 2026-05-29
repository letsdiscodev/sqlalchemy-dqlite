"""Pin: ``do_begin``'s finally closes the cursor even when ``execute("BEGIN")`` raises
CancelledError (greenlet-cancelled mid-BEGIN under the async dialect)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.base import DqliteDialect


def test_do_begin_closes_cursor_on_cancel() -> None:
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = asyncio.CancelledError("greenlet cancel")
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(asyncio.CancelledError):
        dialect.do_begin(mock_conn)

    mock_cursor.close.assert_called_once_with()


def test_do_begin_does_not_mask_begin_exception_with_close_failure() -> None:
    """A transport-class close-time failure must not replace the BEGIN-time exception (the
    finally swallows it with a DEBUG log)."""
    from dqliteclient.exceptions import DqliteConnectionError
    from dqlitedbapi.exceptions import OperationalError

    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = OperationalError("leader flip during BEGIN")
    mock_cursor.close.side_effect = DqliteConnectionError("transport reset")
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(OperationalError, match="leader flip during BEGIN"):
        dialect.do_begin(mock_conn)
