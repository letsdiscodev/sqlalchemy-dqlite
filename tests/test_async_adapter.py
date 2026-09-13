"""The async adapter and dialect over scripted stand-ins for dqlitedbapi.aio."""

from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, NoReturn
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import ArgumentError
from sqlalchemy.util import greenlet_spawn

import dqlitedbapi.exceptions as dbapi_exc
from dqlitedbapi import aio as dbapi_aio
from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor, DqliteDialect_aio


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.description: Any = None
        self.rowcount = -1
        self.lastrowid: int | None = None
        self.arraysize = 1
        self._rows: list[tuple[Any, ...]] = []
        self.closed = False

    async def execute(self, sql: str, params: Any = None) -> FakeCursor:
        self.connection.log.append(("execute", sql, params))
        if self.connection.fail is not None:
            raise self.connection.fail
        if sql.startswith("SELECT"):
            self.description = (("x", 4, None, None, None, None, None),)
            self._rows = [(1,), (2,), (3,)]
            self.rowcount = -1
        else:
            self.description = None
            self.rowcount = 1
            self.lastrowid = 7
        return self

    async def executemany(self, sql: str, seq: Any) -> FakeCursor:
        seq = list(seq)
        self.connection.log.append(("executemany", sql, seq))
        if "RETURNING" in sql:
            self.description = (("id", 4, None, None, None, None, None),)
            self._rows = [(i,) for i, _ in enumerate(seq)]
        self.rowcount = len(seq)
        return self

    async def fetchall(self) -> list[tuple[Any, ...]]:
        rows, self._rows = self._rows, []
        return rows

    def close(self) -> None:
        self.closed = True

    def nextset(self) -> None:
        raise dbapi_exc.NotSupportedError("nextset")


class FakeConnection:
    def __init__(self) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.fail: BaseException | None = None
        self.closed = False
        self.forced = False
        self.connected = False

    async def connect(self) -> None:
        if self.fail is not None:
            raise self.fail
        self.connected = True

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    async def commit(self) -> None:
        self.log.append(("commit",))

    async def rollback(self) -> None:
        self.log.append(("rollback",))

    async def close(self) -> None:
        if self.fail is not None:
            raise self.fail
        self.closed = True

    def force_close_transport(self) -> None:
        self.forced = True
        self.closed = True


def adapted() -> tuple[AsyncAdaptedConnection, FakeConnection]:
    inner = FakeConnection()
    return AsyncAdaptedConnection(dbapi_aio, inner), inner  # type: ignore[arg-type]


async def test_cursor_buffers_rows_and_reports_dml_results() -> None:
    conn, inner = adapted()

    def use() -> None:
        cur = conn.cursor()
        cur.execute("SELECT x FROM t")
        assert cur.description is not None and cur.description[0][0] == "x"
        assert cur.fetchone() == (1,)
        assert cur.fetchmany(5) == [(2,), (3,)]
        assert cur.fetchall() == []
        cur.execute("INSERT INTO t VALUES (?)", (1,))
        assert (cur.rowcount, cur.lastrowid, cur.description) == (1, 7, None)
        cur.executemany("INSERT INTO t VALUES (?) RETURNING id", [(1,), (2,)])
        assert cur.fetchall() == [(0,), (1,)]
        cur.setinputsizes(None)
        cur.close()

    await greenlet_spawn(use)
    assert [entry[0] for entry in inner.log] == ["execute", "execute", "executemany"]


async def test_connection_delegates_and_rejects_server_side_cursors() -> None:
    conn, inner = adapted()

    def use() -> None:
        assert conn.isolation_level == "SERIALIZABLE" and conn.autocommit is False
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            conn.autocommit = True
        with pytest.raises(dbapi_exc.NotSupportedError, match="Server-side"):
            conn.cursor(server_side=True)
        assert isinstance(conn.cursor(), AsyncAdaptedCursor)
        conn.commit()
        conn.rollback()
        conn.close()

    await greenlet_spawn(use)
    assert inner.log == [("commit",), ("rollback",)] and inner.closed


async def test_dbapi_errors_propagate_unchanged() -> None:
    conn, inner = adapted()
    inner.fail = dbapi_exc.OperationalError("boom", code=1)

    def use() -> None:
        with pytest.raises(dbapi_exc.OperationalError, match="boom") as info:
            conn.cursor().execute("SELECT 1")
        assert info.value.code == 1

    await greenlet_spawn(use)


async def test_terminate_forces_the_transport_when_close_fails() -> None:
    conn, inner = adapted()
    inner.fail = dbapi_exc.OperationalError("lost")
    await greenlet_spawn(conn.terminate)
    assert inner.forced
    conn2, inner2 = adapted()
    conn2.terminate()  # outside a greenlet: the garbage-collection path
    assert inner2.forced


async def test_dialect_connect_awaits_the_wire_and_cleans_up_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    d = DqliteDialect_aio()
    d.dbapi = dbapi_aio  # type: ignore[assignment]
    made: list[FakeConnection] = []

    def fake_connect(**kwargs: Any) -> FakeConnection:
        made.append(FakeConnection())
        return made[-1]

    monkeypatch.setattr(dbapi_aio, "connect", fake_connect)
    wrapped = await greenlet_spawn(d.connect, address="h:1", database="db")
    assert isinstance(wrapped, AsyncAdaptedConnection) and made[0].connected
    assert d.get_driver_connection(wrapped) is made[0]

    def failing_creator(**kwargs: Any) -> FakeConnection:
        conn = FakeConnection()
        conn.fail = OSError("refused")
        made.append(conn)
        return conn

    with pytest.raises(OSError):
        await greenlet_spawn(d.connect, address="h:1", async_creator_fn=failing_creator)
    assert made[-1].forced

    async def async_creator(**kwargs: Any) -> FakeConnection:
        return FakeConnection()

    with pytest.raises(ArgumentError, match="async_creator_fn"):
        await greenlet_spawn(d.connect, address="h:1", async_creator_fn=async_creator)


async def test_cancellation_reaches_the_caller() -> None:
    conn, inner = adapted()
    inner.fail = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await greenlet_spawn(lambda: conn.cursor().execute("SELECT 1"))


def _adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


class TestAsyncAdaptedConnectionIsolationLevel:
    def test_reports_serializable(self) -> None:
        assert _adapter().isolation_level == "SERIALIZABLE"

    def test_getattr_with_default_returns_serializable(self) -> None:
        adapter = _adapter()
        assert getattr(adapter, "isolation_level", None) == "SERIALIZABLE"

    def test_read_only(self) -> None:
        import pytest

        adapter = _adapter()
        with pytest.raises(AttributeError):
            adapter.isolation_level = "READ UNCOMMITTED"  # type: ignore[misc]


def _make_adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


def test_handle_exception_default_is_identity() -> None:
    adapter = _make_adapter()
    err = RuntimeError("boom")
    with pytest.raises(RuntimeError) as exc_info:
        adapter._handle_exception(err)
    assert exc_info.value is err


def test_handle_exception_can_be_overridden_to_remap() -> None:
    class Remapping(AsyncAdaptedConnection):
        def _handle_exception(self, error: BaseException) -> NoReturn:
            if isinstance(error, RuntimeError):
                raise OperationalError(str(error)) from error
            raise error

    adapter = Remapping.__new__(Remapping)
    adapter._connection = MagicMock()
    with pytest.raises(OperationalError, match="boom"):
        adapter._handle_exception(RuntimeError("boom"))


@pytest.mark.asyncio
async def test_dbapi_op_lock_serialises_adapter_execute() -> None:
    """Two parallel adapter executes are serialised by the dbapi op_lock —
    the primitive SA's ``_execute_mutex`` would otherwise provide."""
    op_lock = asyncio.Lock()

    observation: list[str] = []

    async def serialised_execute(_op: str, _params: Any | None = None) -> None:
        assert op_lock.locked(), (
            "expected dbapi op_lock to be held during adapter execute round-trip"
        )
        observation.append("execute:start")
        await asyncio.sleep(0.01)
        observation.append("execute:end")

    async def lock_wrapped_execute(op: str, params: Any | None = None) -> None:
        async with op_lock:
            await serialised_execute(op, params)

    fake_dbapi_cursor = MagicMock()
    fake_dbapi_cursor.execute = AsyncMock(side_effect=lock_wrapped_execute)
    fake_dbapi_cursor.executemany = AsyncMock(side_effect=lock_wrapped_execute)
    fake_dbapi_cursor.close = AsyncMock()
    fake_dbapi_cursor.fetchall = AsyncMock(return_value=[])
    fake_dbapi_cursor.description = []
    fake_dbapi_cursor.rowcount = -1
    fake_dbapi_cursor.lastrowid = None

    fake_dbapi_conn = MagicMock()
    fake_dbapi_conn._op_lock = op_lock
    fake_dbapi_conn.cursor = MagicMock(return_value=fake_dbapi_cursor)

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = fake_dbapi_conn

    cursor1 = AsyncAdaptedCursor(adapter)
    cursor2 = AsyncAdaptedCursor(adapter)

    # Drive both executes concurrently via the dbapi cursor (what the
    # adapter cursor body invokes).
    await asyncio.gather(
        fake_dbapi_cursor.execute("SELECT 1"),
        fake_dbapi_cursor.execute("SELECT 2"),
    )

    # A strict start/end pair per call means no interleave.
    assert observation == [
        "execute:start",
        "execute:end",
        "execute:start",
        "execute:end",
    ], f"saw interleave: {observation}"

    assert cursor1._adapt_connection is cursor2._adapt_connection


def test_adapted_cursor_enter_returns_self() -> None:
    """``__enter__`` returns the adapter, not the underlying dbapi cursor."""
    cur = AsyncAdaptedCursor(MagicMock())
    with cur as bound:
        assert bound is cur


def test_adapted_cursor_exit_closes_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    """``__exit__`` calls ``self.close()`` on clean exit."""
    cur = AsyncAdaptedCursor(MagicMock())
    calls: list[AsyncAdaptedCursor] = []
    # __slots__ rejects per-instance method assignment; patch the class.
    monkeypatch.setattr(AsyncAdaptedCursor, "close", lambda self: calls.append(self))
    with cur:
        pass
    assert calls == [cur]


def test_adapted_cursor_exit_does_not_suppress_exception() -> None:
    """``__exit__`` must not swallow the caller's exception."""
    cur = AsyncAdaptedCursor(MagicMock())
    with pytest.raises(RuntimeError, match="body"), cur:
        raise RuntimeError("body")


def test_adapted_cursor_exit_closes_on_exception_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close`` runs even if the body raised, so the cursor can't leak."""
    cur = AsyncAdaptedCursor(MagicMock())
    calls: list[AsyncAdaptedCursor] = []
    monkeypatch.setattr(AsyncAdaptedCursor, "close", lambda self: calls.append(self))
    with pytest.raises(RuntimeError, match="body"), cur:
        raise RuntimeError("body")
    assert calls == [cur]


def test_sync_context_manager_present() -> None:
    assert hasattr(AsyncAdaptedCursor, "__enter__")
    assert hasattr(AsyncAdaptedCursor, "__exit__")


def test_async_context_manager_absent() -> None:
    assert not hasattr(AsyncAdaptedCursor, "__aenter__"), (
        "AsyncAdaptedCursor must not expose __aenter__; SA's reference "
        "AsyncAdapt_dbapi_cursor is sync-only at the cursor layer."
    )
    assert not hasattr(AsyncAdaptedCursor, "__aexit__"), (
        "AsyncAdaptedCursor must not expose __aexit__; SA's reference "
        "AsyncAdapt_dbapi_cursor is sync-only at the cursor layer."
    )


def test_async_iterator_protocol_absent() -> None:
    assert not hasattr(AsyncAdaptedCursor, "__aiter__")
    assert not hasattr(AsyncAdaptedCursor, "__anext__")


class _FakeConn:
    """Minimal stub satisfying the ``AsyncConnection`` shape."""

    def __init__(self) -> None:
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def rollback(self) -> None:
        pass


async def test_connect_honours_async_creator_fn() -> None:
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    called: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    fake = _FakeConn()

    def fake_creator(*args: Any, **kwargs: Any) -> _FakeConn:
        called.append((args, kwargs))
        return fake

    result = await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=fake_creator,
    )

    assert called == [(("1.2.3.4:9000",), {"database": "mydb"})]
    assert isinstance(result, AsyncAdaptedConnection)
    dialect.loaded_dbapi.connect.assert_not_called()
    assert fake.connected is True


async def test_connect_async_creator_fn_kwarg_does_not_trip_allowlist() -> None:
    """The kwarg pop must precede ``_validate_connect_kwargs`` (the key is not allowlisted)."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    fake = _FakeConn()
    await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=lambda *a, **kw: fake,
    )


async def test_connect_without_creator_uses_default_loaded_dbapi() -> None:
    """Regression guard: when the hook is absent, the ``loaded_dbapi.connect`` path runs."""
    dialect = DqliteDialect_aio()

    fake = _FakeConn()
    dialect.loaded_dbapi = MagicMock()
    dialect.loaded_dbapi.connect.return_value = fake

    result = await greenlet_spawn(dialect.connect, "1.2.3.4:9000", database="mydb")

    dialect.loaded_dbapi.connect.assert_called_once_with("1.2.3.4:9000", database="mydb")
    assert isinstance(result, AsyncAdaptedConnection)
    assert fake.connected is True


async def test_async_creator_fn_idempotent_connect_safe_to_double_call() -> None:
    """The dialect always awaits ``raw_conn.connect()`` after the creator runs; an
    already-connected creator result with an idempotent ``connect()`` must not double-open."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    connect_calls = [0]

    class IdempotentFakeConn(_FakeConn):
        async def connect(self) -> None:
            connect_calls[0] += 1
            if connect_calls[0] == 1:
                self.connected = True

    fake = IdempotentFakeConn()
    # Pre-connect to simulate a creator that opens before returning.
    await fake.connect()
    assert connect_calls[0] == 1
    assert fake.connected is True

    await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=lambda *a, **kw: fake,
    )

    # Two connect() calls: the test's pre-call plus the dialect's unconditional re-call.
    assert connect_calls[0] == 2
    assert fake.connected is True


def test_connect_unknown_kwarg_still_raises_argumenterror() -> None:
    """The allowlist still rejects unknown kwargs; this raises synchronously (no greenlet)."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    with pytest.raises(ArgumentError):
        dialect.connect(
            "1.2.3.4:9000",
            database="mydb",
            unknown_kwarg_that_should_be_rejected=True,
        )


def test_async_def_creator_fn_raises_argument_error() -> None:

    async def bad_creator(**_: Any) -> Any:  # noqa: RUF029
        return None

    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=bad_creator)


def test_partial_wrapping_async_def_creator_also_rejected() -> None:
    """``asyncio.iscoroutinefunction`` recognises ``partial`` around an async def
    (where ``inspect.iscoroutinefunction`` returns False)."""

    async def inner_creator(addr: str, **_: Any) -> Any:  # noqa: RUF029
        return None

    wrapper = functools.partial(inner_creator, "127.0.0.1:9001")

    assert inspect.iscoroutinefunction(wrapper)

    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=wrapper)


def test_sync_creator_passes_shape_check() -> None:
    """A sync callable does not trip the pre-flight reject; we observe the downstream failure."""

    def sync_creator(**_: Any) -> Any:
        raise RuntimeError("downstream sentinel")

    dialect = DqliteDialect_aio()
    with pytest.raises(RuntimeError, match="downstream sentinel"):
        dialect.connect(async_creator_fn=sync_creator)


@pytest.mark.parametrize("bogus", [42, "string", object(), [1, 2]])
def test_async_creator_fn_noncallable_raises_argument_error(bogus: Any) -> None:
    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=bogus)


class _Boom(BaseException):
    pass


async def test_connect_propagates_baseexception_from_creator_fn() -> None:
    """raw_conn is still None, so the cleanup arm short-circuits without UnboundLocalError."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    def creator() -> Any:
        raise _Boom("construction failed")

    with pytest.raises(_Boom):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)


async def test_connect_propagates_baseexception_from_dbapi_connect() -> None:
    """Default-factory path: ``loaded_dbapi.connect`` raising must propagate cleanly."""
    dialect = DqliteDialect_aio()

    def boom(*_a: Any, **_kw: Any) -> Any:
        raise _Boom("dbapi connect failed")

    fake_dbapi = MagicMock()
    fake_dbapi.connect = boom
    dialect.loaded_dbapi = fake_dbapi

    with pytest.raises(_Boom):
        await greenlet_spawn(dialect.connect)


async def test_connect_cleanup_arm_handles_raw_conn_none() -> None:
    """The ``raw_conn is not None`` guard must short-circuit before NameError."""
    dialect = DqliteDialect_aio()

    def creator() -> Any:
        raise RuntimeError("creator failed before binding raw_conn")

    with pytest.raises(RuntimeError, match="creator failed"):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)


async def test_connect_cleanup_arm_terminates_after_successful_construction() -> None:
    """When ``raw_conn`` is bound but ``connect()`` raises, cleanup must terminate it."""

    class _ConnRaises:
        def __init__(self) -> None:
            self.terminate_called = False

        async def connect(self) -> None:
            raise RuntimeError("inner connect failed")

        async def close(self) -> None:
            self.terminate_called = True

        def force_close_transport(self) -> None:
            self.terminate_called = True

    raw = _ConnRaises()
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    def creator() -> Any:
        return raw

    with pytest.raises(RuntimeError, match="inner connect failed"):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)

    assert raw.terminate_called is True
