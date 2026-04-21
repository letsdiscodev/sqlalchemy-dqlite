"""Pin ``has_terminate = True`` and the terminate integration path.

SQLAlchemy's async pool gates its forced-disposal path on the dialect's
``has_terminate`` flag plus a working ``do_terminate(dbapi_connection)``
override. Without both, ``engine.dispose()`` on a stuck connection
defers to the full graceful-close path and can block on a hanging
rollback. These tests fence three invariants:

1. The flag is pinned locally on ``DqliteDialect_aio`` (not merely
   inherited from ``DefaultDialect`` / parent MRO).
2. ``do_terminate`` delegates to the adapter's ``terminate()``.
3. ``terminate()`` closes the underlying connection without first
   invoking rollback — the whole point of ``terminate`` is to skip
   any graceful teardown that could hang.
"""

from __future__ import annotations

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _FakeAsyncConn:
    def __init__(self) -> None:
        self.rollback_calls = 0
        self.close_calls = 0

    async def rollback(self) -> None:  # pragma: no cover — must not be called
        self.rollback_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


class TestHasTerminatePinned:
    def test_flag_is_true(self) -> None:
        assert DqliteDialect_aio.has_terminate is True

    def test_flag_is_local_to_class(self) -> None:
        """The pin must live in ``DqliteDialect_aio.__dict__`` so an
        upstream DefaultDialect default flip cannot silently revert it.
        """
        assert "has_terminate" in DqliteDialect_aio.__dict__

    def test_do_terminate_is_local_override(self) -> None:
        assert "do_terminate" in DqliteDialect_aio.__dict__


class TestAsyncAdaptedConnectionTerminate:
    def test_terminate_skips_rollback(self) -> None:
        """``terminate()`` must NOT call rollback — that's the whole
        point of the forced-disposal path. Stub ``await_only`` so the
        sync-driven call resolves deterministically without a real
        event loop.
        """
        fake = _FakeAsyncConn()
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]

        from sqlalchemydqlite import aio as aio_module

        def _fake_await_only(coro: object) -> object:
            import asyncio

            return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

        orig = aio_module.await_only
        aio_module.await_only = _fake_await_only  # type: ignore[assignment]
        try:
            adapter.terminate()
        finally:
            aio_module.await_only = orig  # type: ignore[assignment]

        assert fake.rollback_calls == 0
        assert fake.close_calls == 1


class TestDoTerminateDelegatesToAdapter:
    def test_do_terminate_calls_terminate_on_dbapi_connection(self) -> None:
        """``dialect.do_terminate(dbapi_conn)`` is SQLAlchemy's single
        integration point; verify it forwards to the adapter's
        ``terminate()`` rather than reaching into private state.
        """
        calls: list[str] = []

        class _DbapiConn:
            def terminate(self) -> None:
                calls.append("terminate")

        dialect = DqliteDialect_aio()
        dialect.do_terminate(_DbapiConn())

        assert calls == ["terminate"]

    def test_do_terminate_leaves_rollback_alone(self) -> None:
        """Under a mocked connection, do_terminate must not invoke
        rollback even as a side-effect of teardown logic elsewhere."""
        fake = _FakeAsyncConn()
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
        dialect = DqliteDialect_aio()

        from sqlalchemydqlite import aio as aio_module

        def _fake_await_only(coro: object) -> object:
            import asyncio

            return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

        orig = aio_module.await_only
        aio_module.await_only = _fake_await_only  # type: ignore[assignment]
        try:
            dialect.do_terminate(adapter)
        finally:
            aio_module.await_only = orig  # type: ignore[assignment]

        assert fake.rollback_calls == 0
        assert fake.close_calls == 1
