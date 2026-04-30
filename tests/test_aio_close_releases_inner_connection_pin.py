"""Pin: ``AsyncAdaptedConnection.close()`` and
``AsyncAdaptedCursor.close()`` must release their strong
back-references so a closed adapter retained by SA's pool
diagnostics ring / pytest session-fixture cache does not pin
the inner dbapi ``AsyncConnection`` (and through it the
client-layer state, registered finalizers, and traceback
references on ``_invalidation_cause``).

Mirrors the dbapi-layer fix on ``Cursor.close`` /
``AsyncCursor.close`` (cursor → connection back-reference)
and ``AsyncConnection.close`` finally clause (inner conn
clear). SA's reference adapter (``aiosqlite``) does not do
this — but the dqlite stack's amplification factor
(registered weakref.finalize, loop-bound primitives, frame-
pinning ``_invalidation_cause``) makes the discipline
materially worth applying.

``weakref.proxy`` preserves the API (calls forward to the
inner while it is alive) — only after the inner is
genuinely GC'd does ``ReferenceError`` surface, which is
benign post-close.
"""

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
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]

    # Stub out ``await_only`` and ``in_greenlet`` so close() runs
    # in-process without a real greenlet context.
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
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    cur = AsyncAdaptedCursor(adapter)

    # Stub out ``await_only`` and ``in_greenlet`` so adapter.close()
    # runs in-process without a real greenlet context.
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
    # The closed adapter is left reachable (mirrors SA's pool
    # diagnostic ring / pytest fixture cache pattern). Verify that
    # the inner dbapi AsyncConnection is GC'able anyway — both
    # adapter and cursor must have dropped their strong refs.
    gc.collect()

    assert inner_ref() is None, (
        "Closed adapter and cursor (held by SA's pool diagnostic "
        "ring / pytest fixture cache) must not pin the inner dbapi "
        "AsyncConnection — both layers must drop their strong refs "
        "on close. The cursor captures ``adapt_connection._connection`` "
        "at construction (a separate strong ref that bypasses the "
        "adapter's own slot) — both paths must be released."
    )
