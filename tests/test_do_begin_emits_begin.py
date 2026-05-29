"""``do_begin`` emits an explicit ``BEGIN``; the parent is ``pass`` (pysqlite
auto-begins, the dqlite dbapi does not, so without this each statement
auto-commits and ``engine.begin()`` blocks are not atomic).
"""

from __future__ import annotations

import logging
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
        """The async dialect inherits do_begin from the base class."""
        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        dialect.do_begin(mock_conn)

        mock_cursor.execute.assert_called_once_with("BEGIN")
        mock_cursor.close.assert_called_once_with()


class TestDoBeginErrorHandling:
    def test_closes_cursor_on_execute_failure(self) -> None:
        """The ``finally`` closes the cursor even when BEGIN raises."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError("boom", code=1)
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(dqlitedbapi.exceptions.OperationalError):
            dialect.do_begin(mock_conn)

        mock_cursor.close.assert_called_once_with()

    @pytest.mark.parametrize(
        "close_exc",
        [
            dqlitedbapi.exceptions.OperationalError("close transport reset", code=None),
            dqlitedbapi.exceptions.InterfaceError("close iface fault"),
            dqliteclient.exceptions.DqliteConnectionError("close peer rst"),
            ConnectionResetError("close FIN"),
        ],
        ids=[
            "OperationalError",
            "InterfaceError",
            "DqliteConnectionError",
            "OSError-subclass",
        ],
    )
    def test_close_failure_does_not_mask_begin_failure(
        self,
        close_exc: BaseException,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A transport-class close failure must be swallowed so the
        BEGIN-time exception propagates intact on ``__cause__`` (where SA's
        is_disconnect cause-walk looks); ``__context__`` is not consulted.
        """
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        begin_exc = dqlitedbapi.exceptions.OperationalError("begin sentinel", code=42)
        mock_cursor.execute.side_effect = begin_exc
        mock_cursor.close.side_effect = close_exc
        mock_conn.cursor.return_value = mock_cursor

        with (
            caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"),
            pytest.raises(dqlitedbapi.exceptions.OperationalError) as exc_info,
        ):
            dialect.do_begin(mock_conn)

        assert exc_info.value is begin_exc
        debug_records = [
            r
            for r in caplog.records
            if r.levelname == "DEBUG"
            and "do_begin" in r.message
            and "cursor.close failed" in r.message
        ]
        assert debug_records, (
            f"expected DEBUG log on close-failure suppression; got "
            f"{[r.message for r in caplog.records]}"
        )

    def test_close_failure_non_force_close_tail_re_raises(self) -> None:
        """A close exception outside ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` (e.g.
        ``AttributeError``) must NOT be swallowed."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        begin_exc = dqlitedbapi.exceptions.OperationalError("begin sentinel", code=42)
        mock_cursor.execute.side_effect = begin_exc
        mock_cursor.close.side_effect = AttributeError("cursor lost its close hook")
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(AttributeError, match="cursor lost its close hook") as exc_info:
            dialect.do_begin(mock_conn)
        # BEGIN exception preserved on __context__ (catches ``raise from None``).
        assert exc_info.value.__context__ is begin_exc

    def test_propagates_disconnect_errors(self) -> None:
        """Transport-level BEGIN failures propagate so SA can route through
        is_disconnect to pool-invalidation."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError(
            "Not connected", code=None
        )
        # Wire DqliteConnectionError as __cause__ for is_disconnect's chain walk.
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
        assert dialect.is_disconnect(exc_info.value, None, None) is True
