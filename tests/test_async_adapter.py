"""Tests for async adapter cursor behavior."""

import inspect
from collections import deque
from unittest.mock import MagicMock, patch

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


@pytest.fixture(autouse=True)
def _simulated_greenlet_for_patched_await_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pretend we're in an SA greenlet so the ``in_greenlet()`` preflight
    on close()/terminate() doesn't short-circuit past the patched
    ``await_only`` to the sync force-close path."""
    from sqlalchemydqlite import aio as aio_module

    monkeypatch.setattr(aio_module, "in_greenlet", lambda: True)


def _make_cursor() -> AsyncAdaptedCursor:
    mock_conn = MagicMock()
    adapted_conn = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted_conn._connection = mock_conn
    cursor = AsyncAdaptedCursor(adapted_conn)
    return cursor


def _run_sync(coro_or_value: object) -> object:
    """Replacement for await_only that resolves coroutines synchronously."""
    if hasattr(coro_or_value, "__await__") or hasattr(coro_or_value, "cr_await"):
        # Mocks return plain values, so a single send(None) resolves them.
        try:
            coro_or_value.send(None)  # type: ignore[attr-defined]
        except StopIteration as e:
            return e.value
    return coro_or_value


class TestAsyncAdaptedCursorRowsCleared:
    def test_rows_cleared_after_non_query_execute(self) -> None:
        """After a SELECT then an INSERT, fetchone() must return None."""
        cursor = _make_cursor()

        cursor._rows = deque([(1, "alice")])

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

        cursor._rows = deque([(1, "alice"), (2, "bob")])

        mock_inner = MagicMock()
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
        """executemany with RETURNING must drain accumulated rows into
        self._rows (mirroring execute), not drop them."""
        cursor = _make_cursor()

        mock_inner = MagicMock()
        returned_rows = [(1, "a"), (2, "b"), (3, "c")]
        description = [
            ("id", 1, None, None, None, None, None),
            ("x", 3, None, None, None, None, None),
        ]
        mock_inner.description = description
        mock_inner.executemany.return_value = None
        # Adapter consumes via ``drain_rows`` (ownership-transfer), not
        # ``fetchall``, to avoid an intermediate copy.
        mock_inner.drain_rows.return_value = returned_rows
        mock_inner.close.return_value = None
        # Mirror the RETURNING-path rowcount so result.rowcount isn't -1.
        mock_inner.rowcount = len(returned_rows)
        mock_inner.lastrowid = 3
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.executemany(
                "INSERT INTO t (x) VALUES (?) RETURNING id, x",
                [("a",), ("b",), ("c",)],
            )

        assert cursor.description == description
        assert cursor.rowcount == len(returned_rows)
        assert cursor.lastrowid == 3
        assert cursor.fetchone() == (1, "a")
        assert cursor.fetchone() == (2, "b")
        assert cursor.fetchone() == (3, "c")
        assert cursor.fetchone() is None

    def test_execute_returning_captures_rows_rowcount_and_lastrowid(self) -> None:
        """Single-execute RETURNING copies rowcount and lastrowid alongside
        description and rows, so SA's Result layer sees the affected count."""
        cursor = _make_cursor()

        mock_inner = MagicMock()
        returned_rows = [(7, "alice")]
        description = [
            ("id", 1, None, None, None, None, None),
            ("name", 3, None, None, None, None, None),
        ]
        mock_inner.description = description
        mock_inner.execute.return_value = None
        mock_inner.drain_rows.return_value = returned_rows
        mock_inner.close.return_value = None
        mock_inner.rowcount = 1
        mock_inner.lastrowid = 7
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.execute("INSERT INTO t (name) VALUES (?) RETURNING id, name", ("alice",))

        assert cursor.description == description
        assert cursor.rowcount == 1
        assert cursor.lastrowid == 7
        assert cursor.fetchone() == (7, "alice")
        assert cursor.fetchone() is None


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


class TestAsyncAdaptedConnectionClose:
    """close() rolls back before closing, so an open server-side transaction
    doesn't dangle under NullPool; rollback failure must not prevent close."""

    def test_close_attempts_rollback_first(self) -> None:
        mock_conn = MagicMock()
        calls: list[str] = []
        mock_conn.rollback.side_effect = lambda: calls.append("rollback") or object()  # type: ignore[func-returns-value]
        mock_conn.close.side_effect = lambda: calls.append("close") or object()  # type: ignore[func-returns-value]

        adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapted._connection = mock_conn

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapted.close()

        assert calls == ["rollback", "close"], f"Expected rollback then close, got {calls}"

    def test_close_proceeds_when_rollback_raises_connection_error(self) -> None:
        """A connection-level rollback failure must not block close."""
        from dqlitedbapi.exceptions import OperationalError

        mock_conn = MagicMock()
        calls: list[str] = []

        def failing_rollback() -> None:
            calls.append("rollback-attempt")
            raise OperationalError("simulated rollback failure")

        mock_conn.rollback.side_effect = failing_rollback
        mock_conn.close.side_effect = lambda: calls.append("close") or object()  # type: ignore[func-returns-value]

        adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapted._connection = mock_conn

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            adapted.close()  # must not raise

        assert "close" in calls, "close() must run even if rollback raised"

    def test_close_propagates_programming_error_from_rollback(self) -> None:
        """A rollback failure that looks like a programming bug
        (AttributeError / TypeError / bare RuntimeError) must propagate."""
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


class TestAioDialectConnectCleanup:
    """DqliteDialect_aio.connect() eagerly opens the TCP connection; if that
    raises (or is cancelled mid-connect), the partial raw_conn must be closed
    so it doesn't leak."""

    def test_close_is_called_when_eager_connect_fails(self) -> None:
        import pytest

        from dqliteclient.exceptions import DqliteConnectionError
        from sqlalchemydqlite.aio import DqliteDialect_aio

        dialect = DqliteDialect_aio()

        raw_conn = MagicMock()
        raw_conn.connect = MagicMock(side_effect=DqliteConnectionError("probe failed"))
        raw_conn.close = MagicMock()

        dbapi = MagicMock()
        dbapi.connect = MagicMock(return_value=raw_conn)
        dialect.loaded_dbapi = dbapi

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(DqliteConnectionError, match="probe failed"),
        ):
            dialect.connect()

        raw_conn.close.assert_called_once()

    def test_close_is_called_on_cancellation(self) -> None:
        """CancelledError during eager connect triggers cleanup and still
        propagates (structured-concurrency signal fidelity)."""
        import asyncio

        import pytest

        from sqlalchemydqlite.aio import DqliteDialect_aio

        dialect = DqliteDialect_aio()

        raw_conn = MagicMock()
        raw_conn.connect = MagicMock(side_effect=asyncio.CancelledError())
        raw_conn.close = MagicMock()

        dbapi = MagicMock()
        dbapi.connect = MagicMock(return_value=raw_conn)
        dialect.loaded_dbapi = dbapi

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(asyncio.CancelledError),
        ):
            dialect.connect()

        raw_conn.close.assert_called_once()


class TestAioAllExports:
    """Adapter classes are part of the supported public surface."""

    def test_all_includes_adapter_classes(self) -> None:
        import sqlalchemydqlite.aio as aio_mod

        expected = {"AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"}
        assert expected.issubset(set(aio_mod.__all__))


class TestAioCursorSetInputSizes:
    """``setinputsizes`` accepts both PEP 249's single-sequence shape and
    SA's variadic shape (the wide accept-arm avoids a TypeError on SA's
    internal variadic call)."""

    def test_accepts_single_sequence_argument(self) -> None:
        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        cursor = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
        # __new__ skips __init__; seed the _closed flag the guard reads.
        cursor._closed = False
        cursor.setinputsizes([10, None, 20])

    def test_accepts_variadic_sa_shape(self) -> None:
        """SA's connector reference uses ``setinputsizes(*inputsizes)``."""
        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        cursor = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
        cursor._closed = False
        cursor.setinputsizes(10, None, 20)
        cursor.setinputsizes()  # empty variadic


class TestAioAdapterReturnAnnotations:
    """Lock in the narrower return annotations on the adapter surface."""

    def test_execute_and_executemany_return_none(self) -> None:

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        sig = inspect.signature(AsyncAdaptedCursor.execute)
        assert sig.return_annotation is None or sig.return_annotation == "None"

        sig = inspect.signature(AsyncAdaptedCursor.executemany)
        assert sig.return_annotation is None or sig.return_annotation == "None"

    def test_execute_parameters_narrowed(self) -> None:
        """``parameters`` must not widen back to ``Any``, so mypy --strict
        can warn callers passing a bare ``str`` or a mapping."""
        import typing

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        hints = typing.get_type_hints(AsyncAdaptedCursor.execute)
        annotation = hints["parameters"]
        assert annotation is not typing.Any, "execute(parameters) must not be typed as bare Any"

    def test_executemany_seq_of_parameters_narrowed(self) -> None:
        import typing

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        hints = typing.get_type_hints(AsyncAdaptedCursor.executemany)
        annotation = hints["seq_of_parameters"]
        assert annotation is not typing.Any, (
            "executemany(seq_of_parameters) must not be typed as bare Any"
        )

    def test_iter_returns_self(self) -> None:
        """``__iter__`` returns ``Self`` (PEP 673) so subclass typing is
        preserved through ``iter(cursor)``."""

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        sig = inspect.signature(AsyncAdaptedCursor.__iter__)
        assert "Self" in str(sig.return_annotation)

    def test_next_returns_row_tuple(self) -> None:
        """``__next__`` returns ``tuple[Any, ...]``, not bare ``Any``."""

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        sig = inspect.signature(AsyncAdaptedCursor.__next__)
        assert "tuple" in str(sig.return_annotation)


class TestAioAdapterCursorFetchMethods:
    """Direct unit tests for the fetch* deque semantics — SA's Result layer
    bypasses these entry points, so they only matter to non-SA consumers."""

    def _cursor_with_rows(self, rows: list[tuple[object, ...]]) -> object:
        from collections import deque

        from sqlalchemydqlite.aio import AsyncAdaptedCursor

        cursor = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
        cursor._rows = deque(rows)
        cursor.arraysize = 1
        # Fetch methods enforce a closed-state check; seed it open.
        cursor._closed = False
        return cursor

    def test_fetchone_pops_left(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,)])
        assert cursor.fetchone() == (1,)  # type: ignore[attr-defined]
        assert cursor.fetchone() == (2,)  # type: ignore[attr-defined]
        assert cursor.fetchone() == (3,)  # type: ignore[attr-defined]
        assert cursor.fetchone() is None  # type: ignore[attr-defined]

    def test_fetchone_return_annotation_admits_none(self) -> None:
        """Pin ``fetchone -> tuple[Any, ...] | None`` against silent
        widening to ``Any``."""
        import typing

        hints = typing.get_type_hints(AsyncAdaptedCursor.fetchone)
        args = typing.get_args(hints["return"])
        non_none = [a for a in args if a is not type(None)]
        assert type(None) in args
        assert len(non_none) == 1
        tuple_arg = non_none[0]
        assert typing.get_origin(tuple_arg) is tuple
        assert typing.get_args(tuple_arg) == (typing.Any, ...)

    def test_fetchmany_default_uses_arraysize(self) -> None:
        cursor = self._cursor_with_rows([(1,), (2,), (3,), (4,)])
        cursor.arraysize = 2  # type: ignore[attr-defined]
        assert list(cursor.fetchmany()) == [(1,), (2,)]  # type: ignore[attr-defined]
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
        assert list(cursor) == [(1,), (2,), (3,)]  # type: ignore[call-overload]
        assert len(cursor._rows) == 0  # type: ignore[attr-defined]


class TestAioAdapterConnectionDelegations:
    """commit / rollback / cursor are thin sync-over-async shims via
    ``await_only``; awaiting the wrong attribute would silently skip a
    transaction boundary or return the wrong cursor type."""

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


class TestAsyncAdaptedCursorDescriptionConsistency:
    """If row-draining raises mid-call, ``description`` must roll back to
    None — SA's Result layer reads description-set + empty ``_rows`` as a
    legitimately empty result set."""

    def test_fetchall_raise_leaves_description_none(self) -> None:
        import pytest

        cursor = _make_cursor()

        mock_inner = MagicMock()
        mock_inner.description = (("id", 1, None, None, None, None, None),)
        mock_inner.execute.return_value = None
        mock_inner.close.return_value = None
        # ``drain_rows`` raises before returning: the adapter must not commit
        # ``description`` nor leave ``_rows`` half-assigned.
        mock_inner.drain_rows.side_effect = RuntimeError("synthetic drain_rows failure")
        cursor._connection.cursor.return_value = mock_inner

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(RuntimeError, match="synthetic drain_rows failure"),
        ):
            cursor.execute("SELECT id FROM t")

        assert cursor.description is None, (
            "description must roll back to None when fetchall raises, "
            "so SQLAlchemy's Result layer cannot misread the cursor as "
            "holding an empty result set"
        )
        assert len(cursor._rows) == 0


class TestAsyncAdaptedCursorDescriptionType:
    """``cursor.description`` is passed through unconverted, whatever
    sequence type the underlying dbapi cursor returns."""

    def test_description_passes_through_tuple_of_tuples(self) -> None:
        cursor = _make_cursor()

        mock_inner = MagicMock()
        mock_inner.description = (
            ("id", 1, None, None, None, None, None),
            ("name", 3, None, None, None, None, None),
        )
        mock_inner.lastrowid = None
        mock_inner.rowcount = 0
        mock_inner.execute.return_value = None
        mock_inner.fetchall.return_value = []
        mock_inner.close.return_value = None
        cursor._connection.cursor.return_value = mock_inner

        with patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync):
            cursor.execute("SELECT id, name FROM t")

        assert cursor.description == mock_inner.description
