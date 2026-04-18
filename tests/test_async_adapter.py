"""Tests for async adapter cursor behavior."""

import ast
import inspect
import textwrap
from collections import deque
from unittest.mock import MagicMock, patch

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_cursor() -> AsyncAdaptedCursor:
    """Create an AsyncAdaptedCursor with a mocked connection."""
    mock_conn = MagicMock()
    adapted_conn = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted_conn._connection = mock_conn
    cursor = AsyncAdaptedCursor(adapted_conn)
    return cursor


def _run_sync(coro_or_value: object) -> object:
    """Replacement for await_only that resolves coroutines synchronously."""
    if hasattr(coro_or_value, "__await__") or hasattr(coro_or_value, "cr_await"):
        # It's a coroutine -- we can't actually await it in sync context,
        # but our mocks return plain values, so send(None) is enough.
        try:
            coro_or_value.send(None)  # type: ignore[union-attr]
        except StopIteration as e:
            return e.value
    return coro_or_value


class TestAsyncAdaptedCursorRowsCleared:
    def test_rows_cleared_after_non_query_execute(self) -> None:
        """After a SELECT then an INSERT, fetchone() must return None."""
        cursor = _make_cursor()

        # Simulate that a previous SELECT populated _rows
        cursor._rows = deque([(1, "alice")])

        # Set up a mock inner cursor that returns no description (DML)
        mock_inner = MagicMock()
        mock_inner.description = None
        mock_inner.lastrowid = 1
        mock_inner.rowcount = 1
        mock_inner.execute.return_value = None
        mock_inner.close.return_value = None
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.execute("INSERT INTO t VALUES (1)")

        result = cursor.fetchone()
        assert result is None, f"Expected None after non-query execute, got {result}"

    def test_rows_cleared_after_executemany(self) -> None:
        """After executemany(), fetchone() must return None."""
        cursor = _make_cursor()

        # Simulate that a previous SELECT populated _rows
        cursor._rows = deque([(1, "alice"), (2, "bob")])

        mock_inner = MagicMock()
        mock_inner.lastrowid = 3
        mock_inner.rowcount = 2
        mock_inner.executemany.return_value = None
        mock_inner.close.return_value = None
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

        result = cursor.fetchone()
        assert result is None, f"Expected None after executemany, got {result}"


def _has_finally_with_close(func: object) -> bool:
    """Check if a function has cursor.close() inside a finally block."""
    source = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Try) and node.finalbody:
            for stmt in ast.walk(node):
                if isinstance(stmt, ast.Call):
                    func_node = stmt.func
                    if isinstance(func_node, ast.Attribute) and func_node.attr == "close":
                        return True
    return False


class TestAsyncAdaptedCursorCleanup:
    def test_cursor_closed_on_execute_error(self) -> None:
        """Underlying cursor must be closed even if execute() raises."""
        assert _has_finally_with_close(AsyncAdaptedCursor.execute), (
            "cursor.close() should be in a finally block to prevent leaks on error"
        )

    def test_executemany_cursor_closed_on_error(self) -> None:
        """Underlying cursor must be closed even if executemany() raises."""
        assert _has_finally_with_close(AsyncAdaptedCursor.executemany), (
            "cursor.close() should be in a finally block to prevent leaks on error"
        )


class TestAsyncAdaptedConnectionClose:
    """ISSUE-91: close() attempts rollback before closing.

    SQLAlchemy's async adapter previously closed the connection without
    a rollback, leaving any open server-side transaction dangling in
    unpooled / NullPool usage. Verify rollback is called first, and
    verify rollback failure does not prevent close.
    """

    def test_close_attempts_rollback_first(self) -> None:
        mock_conn = MagicMock()
        calls: list[str] = []
        mock_conn.rollback.side_effect = lambda: calls.append("rollback") or object()
        mock_conn.close.side_effect = lambda: calls.append("close") or object()

        adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapted._connection = mock_conn

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapted.close()

        assert calls == ["rollback", "close"], f"Expected rollback then close, got {calls}"

    def test_close_proceeds_when_rollback_raises(self) -> None:
        """A failing rollback (e.g. connection already in a bad state)
        must not block close — resource cleanup is more important."""
        mock_conn = MagicMock()
        calls: list[str] = []

        def failing_rollback() -> None:
            calls.append("rollback-attempt")
            raise RuntimeError("simulated rollback failure")

        mock_conn.rollback.side_effect = failing_rollback
        mock_conn.close.side_effect = lambda: calls.append("close") or object()

        adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapted._connection = mock_conn

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapted.close()  # must not raise

        assert "close" in calls, "close() must run even if rollback raised"
