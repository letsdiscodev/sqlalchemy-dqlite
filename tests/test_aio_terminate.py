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

        orig = aio_module.await_only  # type: ignore[attr-defined]
        _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
        aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
        aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
        try:
            adapter.terminate()
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]

            aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]

        assert fake.rollback_calls == 0
        assert fake.close_calls == 1


class TestTerminateSuppressesTransportExceptions:
    """``terminate()`` is SA's forced-disposal path and
    ``has_terminate = True`` promises it never raises — an unhandled
    exception here would abort ``engine.dispose()`` mid-flight and
    leak subsequent pool slots. The narrow catch tuple
    ``(OperationalError, InterfaceError, DqliteConnectionError,
    OSError)`` must genuinely suppress every class, so a future audit
    that drops one silently re-introduces the leak.

    Pins each class individually so the failure mode is named.
    """

    @staticmethod
    def _swap_await_only() -> tuple[object, object, object]:
        import asyncio

        from sqlalchemydqlite import aio as aio_module

        def _fake(coro: object) -> object:
            return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

        orig = aio_module.await_only  # type: ignore[attr-defined]
        orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
        aio_module.await_only = _fake  # type: ignore[assignment,attr-defined]
        # Pretend we're inside an SA greenlet so the terminate()
        # ``in_greenlet()`` preflight enters the await_only path
        # under test rather than short-circuiting to the sync
        # force-close.
        aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
        return aio_module, orig, orig_in_greenlet

    def _run_terminate(self, close_exc: BaseException) -> _FakeAsyncConnWithExc:
        fake = _FakeAsyncConnWithExc(close_exc=close_exc)
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
        aio_module, orig, orig_in_greenlet = self._swap_await_only()
        try:
            adapter.terminate()  # must not raise
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]
            aio_module.in_greenlet = orig_in_greenlet  # type: ignore[attr-defined]
        return fake

    def test_operational_error_suppressed(self) -> None:
        from dqlitedbapi.exceptions import OperationalError

        fake = self._run_terminate(OperationalError("op fail", code=None))
        assert fake.close_calls == 1

    def test_interface_error_suppressed(self) -> None:
        from dqlitedbapi.exceptions import InterfaceError

        fake = self._run_terminate(InterfaceError("iface fail"))
        assert fake.close_calls == 1

    def test_dqlite_connection_error_suppressed(self) -> None:
        from dqliteclient.exceptions import DqliteConnectionError

        fake = self._run_terminate(DqliteConnectionError("transport down"))
        assert fake.close_calls == 1

    def test_broken_pipe_os_error_suppressed(self) -> None:
        fake = self._run_terminate(OSError(32, "Broken pipe"))
        assert fake.close_calls == 1

    def test_connection_reset_os_error_suppressed(self) -> None:
        fake = self._run_terminate(ConnectionResetError(104, "reset by peer"))
        assert fake.close_calls == 1

    def test_programmer_bug_attribute_error_propagates(self) -> None:
        """Guard against a future audit widening the except list: a
        programmer bug (AttributeError, TypeError, etc.) must still
        escape so a refactor regression doesn't get silently eaten."""
        import pytest

        fake = _FakeAsyncConnWithExc(close_exc=AttributeError("wrong attr"))
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
        aio_module, orig, orig_in_greenlet = self._swap_await_only()
        try:
            with pytest.raises(AttributeError):
                adapter.terminate()
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]
            aio_module.in_greenlet = orig_in_greenlet  # type: ignore[attr-defined]
        # Close was still attempted before the exception escaped.
        assert fake.close_calls == 1


class _FakeAsyncConnWithExc:
    """Mirror of ``_FakeAsyncConn`` that raises from ``close``. Kept
    separate so the happy-path fixture above stays trivial."""

    def __init__(self, close_exc: BaseException | None = None) -> None:
        self._close_exc = close_exc
        self.rollback_calls = 0
        self.close_calls = 0
        self.address = ("10.0.0.1", 9001)

    async def rollback(self) -> None:  # pragma: no cover
        self.rollback_calls += 1

    async def close(self) -> None:
        self.close_calls += 1
        if self._close_exc is not None:
            raise self._close_exc


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

        orig = aio_module.await_only  # type: ignore[attr-defined]
        _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
        aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
        aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
        try:
            dialect.do_terminate(adapter)
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]

            aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]

        assert fake.rollback_calls == 0
        assert fake.close_calls == 1
