"""close() on connection and cursor must drop strong back-refs so a closed
adapter retained by SA's pool ring / pytest fixture cache cannot pin the
inner dbapi AsyncConnection (and through it client state and finalizers)."""

from __future__ import annotations

import gc
import weakref

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


class _FakeAsyncConn:
    address = "localhost:9001"
    is_closed = False

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        self.is_closed = True

    def cursor(self) -> object:
        return object()


def test_async_adapted_connection_close_drops_strong_inner_ref() -> None:
    inner = _FakeAsyncConn()
    inner_ref = weakref.ref(inner)
    adapter = AsyncAdaptedConnection(inner)

    # Stub await_only / in_greenlet so close() runs without a greenlet context.
    from sqlalchemydqlite import aio as aio_module

    async def _no_op() -> None:
        pass

    real_await = aio_module.await_only  # type: ignore[attr-defined]
    real_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]

    def _fake_await(coro: object) -> object:
        if hasattr(coro, "__await__"):
            it = coro.__await__()
            try:
                next(it)
            except StopIteration as e:
                return e.value
        return None

    aio_module.await_only = _fake_await  # type: ignore[attr-defined,assignment]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        adapter.close()
    finally:
        aio_module.await_only = real_await  # type: ignore[attr-defined]
        aio_module.in_greenlet = real_in_greenlet  # type: ignore[attr-defined]

    del inner
    gc.collect()

    assert inner_ref() is None, (
        "AsyncAdaptedConnection.close() must drop the strong "
        "back-reference to the inner dbapi AsyncConnection — "
        "otherwise SA's pool-diagnostic / pytest fixture cache "
        "patterns pin the dbapi connection and through it the "
        "client conn's frame-state on _invalidation_cause."
    )


def test_async_adapted_cursor_close_drops_strong_inner_conn_ref() -> None:
    inner = _FakeAsyncConn()
    adapter = AsyncAdaptedConnection(inner)
    cur = AsyncAdaptedCursor(adapter)

    # Stub await_only / in_greenlet so adapter.close() runs without a greenlet.
    from sqlalchemydqlite import aio as aio_module

    real_await = aio_module.await_only  # type: ignore[attr-defined]
    real_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]

    def _fake_await(coro: object) -> object:
        if hasattr(coro, "__await__"):
            it = coro.__await__()
            try:
                next(it)
            except StopIteration as e:
                return e.value
        return None

    aio_module.await_only = _fake_await  # type: ignore[attr-defined,assignment]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        cur.close()
        adapter.close()
    finally:
        aio_module.await_only = real_await  # type: ignore[attr-defined]
        aio_module.in_greenlet = real_in_greenlet  # type: ignore[attr-defined]

    inner_ref = weakref.ref(inner)
    del inner
    # Adapter left reachable (mirrors SA's pool ring); inner must still be GC'able.
    gc.collect()

    assert inner_ref() is None, (
        "Closed adapter and cursor (held by SA's pool diagnostic "
        "ring / pytest fixture cache) must not pin the inner dbapi "
        "AsyncConnection — both layers must drop their strong refs "
        "on close. The cursor captures ``adapt_connection._connection`` "
        "at construction (a separate strong ref that bypasses the "
        "adapter's own slot) — both paths must be released."
    )
