"""Pin: ``do_begin``'s finally closes the cursor even when
``cursor.execute("BEGIN")`` raises CancelledError.

Existing tests cover OperationalError mid-BEGIN. The CancelledError
case (greenlet-cancelled mid-BEGIN under the async dialect) was
uncovered by the audit — this file pins it.
"""

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
    """Pin: a transport-class close-time failure must NOT replace the
    BEGIN-time exception. ``finally``'s narrow try/except for
    DatabaseError / InterfaceError / DqliteConnectionError / OSError
    swallows the close failure with a DEBUG log — the BEGIN exception
    stays the active one."""
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
