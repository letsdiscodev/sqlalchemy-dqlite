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
        """After a DML executemany(), fetchone() must return None and the
        adapter must reflect the underlying cursor's rowcount / lastrowid.
        """
        cursor = _make_cursor()

        # Simulate that a previous SELECT populated _rows
        cursor._rows = deque([(1, "alice"), (2, "bob")])

        mock_inner = MagicMock()
        # DML: underlying cursor reports no description.
        mock_inner.description = None
        mock_inner.lastrowid = 3
        mock_inner.rowcount = 2
        mock_inner.executemany.return_value = None
        mock_inner.close.return_value = None
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

        assert cursor.fetchone() is None
        assert cursor.lastrowid == 3
        assert cursor.rowcount == 2
        assert cursor.description is None

    def test_executemany_returning_captures_rows_and_description(self) -> None:
        """When executemany is run with a RETURNING clause, the underlying
        cursor accumulates rows across parameter sets. The adapter must
        mirror execute()'s pattern: read description and drain fetchall
        into self._rows so downstream SQLAlchemy result handling sees the
        rows. Before the fix the adapter silently dropped them.
        """
        cursor = _make_cursor()

        mock_inner = MagicMock()
        returned_rows = [(1, "a"), (2, "b"), (3, "c")]
        description = [
            ("id", 1, None, None, None, None, None),
            ("x", 3, None, None, None, None, None),
        ]
        mock_inner.description = description
        mock_inner.executemany.return_value = None
        mock_inner.fetchall.return_value = returned_rows
        mock_inner.close.return_value = None
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.executemany(
                "INSERT INTO t (x) VALUES (?) RETURNING id, x",
                [("a",), ("b",), ("c",)],
            )

        assert cursor.description == description
        assert cursor.fetchone() == (1, "a")
        assert cursor.fetchone() == (2, "b")
        assert cursor.fetchone() == (3, "c")
        assert cursor.fetchone() is None


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


class TestAsyncAdaptedCursorOptionalMethods:
    def test_connection_property_returns_adapter(self) -> None:
        cursor = _make_cursor()
        assert cursor.connection is cursor._adapt_connection

    def test_callproc_raises_not_supported(self) -> None:
        import pytest

        from dqlitedbapi.exceptions import NotSupportedError

        cursor = _make_cursor()
        with pytest.raises(NotSupportedError):
            cursor.callproc("sp_foo")

    def test_nextset_raises_not_supported(self) -> None:
        import pytest

        from dqlitedbapi.exceptions import NotSupportedError

        cursor = _make_cursor()
        with pytest.raises(NotSupportedError):
            cursor.nextset()

    def test_scroll_raises_not_supported(self) -> None:
        import pytest

        from dqlitedbapi.exceptions import NotSupportedError

        cursor = _make_cursor()
        with pytest.raises(NotSupportedError):
            cursor.scroll(5)


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
    """close() attempts rollback before closing.

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

    def test_close_proceeds_when_rollback_raises_connection_error(self) -> None:
        """A failing rollback caused by a connection-level error (e.g.
        broken transport, OS-level disconnect, server already closed)
        must not block close — resource cleanup is more important."""
        from dqlitedbapi.exceptions import OperationalError

        mock_conn = MagicMock()
        calls: list[str] = []

        def failing_rollback() -> None:
            calls.append("rollback-attempt")
            raise OperationalError("simulated rollback failure")

        mock_conn.rollback.side_effect = failing_rollback
        mock_conn.close.side_effect = lambda: calls.append("close") or object()

        adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapted._connection = mock_conn

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapted.close()  # must not raise

        assert "close" in calls, "close() must run even if rollback raised"

    def test_close_propagates_programming_error_from_rollback(self) -> None:
        """A failing rollback that looks like a programming bug
        (AttributeError / TypeError / bare RuntimeError) must propagate
        so refactor regressions don't get swallowed silently."""
        import pytest

        mock_conn = MagicMock()
        mock_conn.rollback.side_effect = AttributeError("refactor bug")
        mock_conn.close.return_value = object()

        adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapted._connection = mock_conn

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(AttributeError),
        ):
            adapted.close()


class TestAioAllExports:
    """Adapter classes are part of the supported public surface."""

    def test_all_includes_adapter_classes(self) -> None:
        import sqlalchemydqlite.aio as aio_mod

        expected = {"AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"}
        assert expected.issubset(set(aio_mod.__all__))


class TestAioCursorSetInputSizes:
    """PEP 249 conformance: setinputsizes takes a single sequence."""

    def test_accepts_single_sequence_argument(self) -> None:
        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        cursor = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
        cursor.setinputsizes([10, None, 20])  # no error

    def test_extra_positional_argument_rejected(self) -> None:
        import pytest

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        cursor = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
        with pytest.raises(TypeError):
            cursor.setinputsizes([10], 20)  # type: ignore[call-arg]


class TestAioAdapterReturnAnnotations:
    """Lock in the narrower return annotations on the async adapter surface."""

    def test_execute_and_executemany_return_none(self) -> None:
        import inspect

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        sig = inspect.signature(AsyncAdaptedCursor.execute)
        assert sig.return_annotation is None or sig.return_annotation == "None"

        sig = inspect.signature(AsyncAdaptedCursor.executemany)
        assert sig.return_annotation is None or sig.return_annotation == "None"

    def test_iter_returns_iterator(self) -> None:
        import inspect

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        sig = inspect.signature(AsyncAdaptedCursor.__iter__)
        # Accept either the Iterator annotation object or the stringified form.
        assert "Iterator" in str(sig.return_annotation)


class TestAioAdapterCursorFetchMethods:
    """The adapter's fetch* methods buffer rows into a deque populated
    by ``execute``. The integration tests exercise them via SQLAlchemy
    Result, which bypasses these entry points entirely — so direct
    unit tests pin the semantics for non-SQLAlchemy consumers.
    """

    def _cursor_with_rows(self, rows: list[tuple[object, ...]]) -> object:
        from collections import deque

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        cursor = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
        cursor._rows = deque(rows)
        cursor.arraysize = 1
        return cursor

    def test_fetchone_pops_left(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,)])
        assert cursor.fetchone() == (1,)  # type: ignore[attr-defined]
        assert cursor.fetchone() == (2,)  # type: ignore[attr-defined]
        assert cursor.fetchone() == (3,)  # type: ignore[attr-defined]
        assert cursor.fetchone() is None  # type: ignore[attr-defined]

    def test_fetchmany_default_uses_arraysize(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,), (4,)])
        cursor.arraysize = 2  # type: ignore[attr-defined]
        assert list(cursor.fetchmany()) == [(1,), (2,)]  # type: ignore[attr-defined]
        # Remaining rows are still available.
        assert list(cursor.fetchmany()) == [(3,), (4,)]  # type: ignore[attr-defined]

    def test_fetchmany_explicit_size(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,)])
        assert list(cursor.fetchmany(2)) == [(1,), (2,)]  # type: ignore[attr-defined]
        assert list(cursor.fetchmany(10)) == [(3,)]  # type: ignore[attr-defined]

    def test_fetchall_drains_deque(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,)])
        assert cursor.fetchall() == [(1,), (2,), (3,)]  # type: ignore[attr-defined]
        assert len(cursor._rows) == 0  # type: ignore[attr-defined]

    def test_iter_drains_deque(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,)])
        assert list(cursor) == [(1,), (2,), (3,)]
        assert len(cursor._rows) == 0  # type: ignore[attr-defined]


class TestAioAdapterConnectionDelegations:
    """AsyncAdaptedConnection.commit / rollback / cursor are thin
    sync-over-async shims via ``await_only``. A refactor that awaits
    the wrong attribute would silently skip transaction boundaries or
    hand back a cursor of the wrong type. Unit tests are cheap.
    """

    def test_commit_delegates_through_await_only(self) -> None:
        from unittest.mock import AsyncMock, MagicMock, patch

        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        inner = MagicMock()
        inner.commit = AsyncMock()
        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapter._connection = inner

        def _run_sync(coro: object) -> object:
            import asyncio

            return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapter.commit()

        inner.commit.assert_awaited_once()

    def test_rollback_delegates_through_await_only(self) -> None:
        from unittest.mock import AsyncMock, MagicMock, patch

        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        inner = MagicMock()
        inner.rollback = AsyncMock()
        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapter._connection = inner

        def _run_sync(coro: object) -> object:
            import asyncio

            return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapter.rollback()

        inner.rollback.assert_awaited_once()

    def test_cursor_returns_async_adapted_cursor_wrapping_inner(self) -> None:
        from unittest.mock import MagicMock

        from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor

        inner = MagicMock()
        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapter._connection = inner

        cursor = adapter.cursor()
        assert isinstance(cursor, AsyncAdaptedCursor)
