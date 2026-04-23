"""AsyncAdaptedCursor close-time suppression narrowed to (Exception,
asyncio.CancelledError).

``contextlib.suppress(BaseException)`` is a documented anti-pattern: it
swallows ``KeyboardInterrupt`` and ``SystemExit`` in addition to the
``asyncio.CancelledError`` the inline comment cites. A Ctrl-C fired
during a long-running query's cleanup must not silently disappear. The
narrow tuple explicitly includes ``CancelledError`` because it subclasses
``BaseException`` (not ``Exception``) since 3.8, so ``Exception`` alone
would not cover the greenlet-cancel case the suppression was installed
for.
"""

from __future__ import annotations

import asyncio
from collections import deque
from unittest.mock import MagicMock, patch

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _run_sync(coro_or_value: object) -> object:
    if hasattr(coro_or_value, "__await__") or hasattr(coro_or_value, "cr_await"):
        try:
            coro_or_value.send(None)  # type: ignore[union-attr]
        except StopIteration as e:
            return e.value
    return coro_or_value


def _make_cursor() -> AsyncAdaptedCursor:
    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = MagicMock()
    return AsyncAdaptedCursor(adapted)


class TestExecuteCloseSuppression:
    def test_keyboard_interrupt_during_close_propagates(self) -> None:
        cursor = _make_cursor()
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.execute.return_value = None
        mock_inner.close.side_effect = KeyboardInterrupt()
        cursor._connection.cursor.return_value = mock_inner

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(KeyboardInterrupt),
        ):
            cursor.execute("INSERT INTO t VALUES (1)")

    def test_system_exit_during_close_propagates(self) -> None:
        cursor = _make_cursor()
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.execute.return_value = None
        mock_inner.close.side_effect = SystemExit(1)
        cursor._connection.cursor.return_value = mock_inner

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(SystemExit),
        ):
            cursor.execute("INSERT INTO t VALUES (1)")

    def test_cancelled_error_during_close_is_suppressed(self) -> None:
        """``asyncio.CancelledError`` is a ``BaseException`` and is the
        explicit motivator for the suppression. Keep covering it."""
        cursor = _make_cursor()
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.execute.return_value = None
        mock_inner.close.side_effect = asyncio.CancelledError()
        cursor._connection.cursor.return_value = mock_inner

        # Must not raise — the primary execute path is considered the
        # "primary" outcome and the close-time cancel must not clobber it.
        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.execute("INSERT INTO t VALUES (1)")

    def test_ordinary_exception_during_close_is_suppressed(self) -> None:
        cursor = _make_cursor()
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.execute.return_value = None
        mock_inner.close.side_effect = RuntimeError("boom")
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.execute("INSERT INTO t VALUES (1)")


class TestExecutemanyCloseSuppression:
    def test_keyboard_interrupt_during_close_propagates(self) -> None:
        cursor = _make_cursor()
        cursor._rows = deque()
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.executemany.return_value = None
        mock_inner.close.side_effect = KeyboardInterrupt()
        cursor._connection.cursor.return_value = mock_inner

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(KeyboardInterrupt),
        ):
            cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    def test_cancelled_error_during_close_is_suppressed(self) -> None:
        cursor = _make_cursor()
        cursor._rows = deque()
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.executemany.return_value = None
        mock_inner.close.side_effect = asyncio.CancelledError()
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
