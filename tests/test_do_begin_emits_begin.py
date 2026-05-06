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
        """The ``finally``-block's narrow ``except _TRANSPORT_CLASS_EXCEPTIONS:``
        must swallow a transport-class close failure so the BEGIN-time
        exception (the one SA's ``is_disconnect`` cause-walk needs to see
        on ``__cause__``) propagates intact. A regression that re-raises
        the close exception, broadens the except, or swaps try/finally
        order would silently misclassify the failure on SA's pool path
        (close-time exception replaces BEGIN-time on the active raise;
        the BEGIN exception lives only on ``__context__``, which the
        cause-walk does NOT consult).

        Sibling test ``test_closes_cursor_on_execute_failure`` covers
        the close-succeeds-during-BEGIN-fails leg; this fills the
        close-also-fails leg across the four transport-class arms.
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

        # The BEGIN-time exception is the active raise, not the close-time
        # one. ``__context__`` will reference the close exception (Python's
        # implicit exception chaining inside the ``finally``), but the
        # *propagating* exception must be the BEGIN one.
        assert exc_info.value is begin_exc
        # The close-time exception should be DEBUG-logged with exc_info.
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

    def test_close_failure_non_transport_class_re_raises(self) -> None:
        """The close-failure suppression is intentionally narrow to
        ``_TRANSPORT_CLASS_EXCEPTIONS``; an out-of-band close exception
        (e.g., ``IntegrityError`` from a custom audit trigger, or a
        plain ``RuntimeError`` from a programmer bug) must NOT be
        swallowed. The narrow except is documented at base.py:1552-1559
        as "let it surface instead of silently masking the BEGIN-time
        exception" — pin the surfacing behaviour so a refactor that
        broadens to ``except Exception:`` regresses it.
        """
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        begin_exc = dqlitedbapi.exceptions.OperationalError("begin sentinel", code=42)
        mock_cursor.execute.side_effect = begin_exc
        # IntegrityError and bare RuntimeError are both NOT in
        # _TRANSPORT_CLASS_EXCEPTIONS — they should escape the narrow
        # except and replace the BEGIN exception.
        mock_cursor.close.side_effect = RuntimeError("close fault out of band")
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(RuntimeError, match="close fault out of band") as exc_info:
            dialect.do_begin(mock_conn)
        # Belt-and-brace: Python's implicit chaining stashes the
        # BEGIN-time exception on the raised RuntimeError's
        # ``__context__`` (because the close happened inside the
        # ``finally`` of the try/except block that caught BEGIN).
        # Pin so a refactor that swaps to ``raise from None`` discards
        # the BEGIN diagnostic from the traceback chain.
        assert exc_info.value.__context__ is begin_exc

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
