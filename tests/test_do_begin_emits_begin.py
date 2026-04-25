"""``DqliteDialect.do_begin`` emits an explicit ``BEGIN`` over the wire.

SA's parent ``SQLiteDialect_pysqlite.do_begin`` is ``pass`` because
pysqlite's stdlib driver auto-emits ``BEGIN`` before the first DML
via the connection-level ``isolation_level`` attribute. The dqlite
dbapi has no such mechanism — without an explicit ``BEGIN`` over
the wire the server auto-commits each statement and
``engine.begin()`` blocks would not be atomic.

These unit tests pin the wire-shape contract:

- ``BEGIN`` literal (plain — not DEFERRED / IMMEDIATE / EXCLUSIVE).
- Cursor opened, ``execute("BEGIN")`` called, cursor closed in a
  ``finally`` so a failed BEGIN does not leak a cursor.
- BEGIN errors propagate (SA's ``Connection._begin_impl`` wraps the
  call so ``is_disconnect`` classification and pool-invalidation
  kick in for transport-level BEGIN failures).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqliteclient.exceptions
import dqlitedbapi.exceptions
from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


class TestDoBeginEmitsBegin:
    def test_sync_dialect_emits_begin(self) -> None:
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        dialect.do_begin(mock_conn)

        mock_conn.cursor.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with("BEGIN")
        mock_cursor.close.assert_called_once_with()

    def test_async_dialect_inherits_emits_begin(self) -> None:
        """The async dialect inherits do_begin from the base class —
        no separate override. Pin the inherited call shape."""
        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        dialect.do_begin(mock_conn)

        mock_cursor.execute.assert_called_once_with("BEGIN")
        mock_cursor.close.assert_called_once_with()


class TestDoBeginErrorHandling:
    def test_closes_cursor_on_execute_failure(self) -> None:
        """The ``finally``-clause must close the cursor even when
        ``execute("BEGIN")`` raises — otherwise a failed BEGIN
        leaks the cursor handle."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError("boom", code=1)
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(dqlitedbapi.exceptions.OperationalError):
            dialect.do_begin(mock_conn)

        mock_cursor.close.assert_called_once_with()

    def test_propagates_disconnect_errors(self) -> None:
        """Transport-level failures during BEGIN must propagate so
        SA's ``Connection._begin_impl`` can route through
        ``_handle_dbapi_exception`` → ``is_disconnect`` →
        pool-invalidation."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError(
            "Not connected", code=None
        )
        # Wire the cause shape that is_disconnect's chain walk relies
        # on (an underlying DqliteConnectionError as __cause__).
        try:
            raise dqliteclient.exceptions.DqliteConnectionError("peer rst")
        except dqliteclient.exceptions.DqliteConnectionError as inner:
            mock_cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError(
                "Not connected", code=None
            )
            mock_cursor.execute.side_effect.__cause__ = inner
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(dqlitedbapi.exceptions.OperationalError) as exc_info:
            dialect.do_begin(mock_conn)
        # The dialect's is_disconnect would walk this chain to find
        # the DqliteConnectionError and classify as disconnect.
        assert dialect.is_disconnect(exc_info.value, None, None) is True
