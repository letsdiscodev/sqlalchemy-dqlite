"""``AsyncAdaptedConnection.close`` final-close transport-error suppression.

``close()`` runs a rollback-then-close sequence inside ``try/finally``.
The final close is narrow-catch on
``(OperationalError, InterfaceError, DqliteConnectionError, OSError)`` —
the same set as ``terminate()``. Existing tests cover the
"rollback raises" branches; this file pins the "final close() itself
raises" branches so a future audit narrowing the catch list cannot
silently propagate a transport error into SA's pool cleanup path.
"""

from __future__ import annotations

import asyncio

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _FakeAsyncConn:
    def __init__(
        self,
        close_exc: BaseException | None,
        rollback_exc: BaseException | None = None,
    ) -> None:
        self._close_exc = close_exc
        self._rollback_exc = rollback_exc
        self.rollback_calls = 0
        self.close_calls = 0
        self.address = ("10.0.0.1", 9001)

    async def rollback(self) -> None:
        self.rollback_calls += 1
        if self._rollback_exc is not None:
            raise self._rollback_exc

    async def close(self) -> None:
        self.close_calls += 1
        if self._close_exc is not None:
            raise self._close_exc


def _swap_await_only() -> tuple[object, object, object]:
    from sqlalchemydqlite import aio as aio_module

    def _fake(coro: object) -> object:
        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig = aio_module.await_only  # type: ignore[attr-defined]
    orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake  # type: ignore[assignment,attr-defined]
    # Pretend we're inside an SA greenlet so the close()/terminate()
    # ``in_greenlet()`` preflight enters the await_only path under
    # test rather than short-circuiting to the sync force-close.
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    return aio_module, orig, orig_in_greenlet


def _run_close(close_exc: BaseException | None) -> _FakeAsyncConn:
    fake = _FakeAsyncConn(close_exc=close_exc)
    adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
    aio_module, orig, _orig_in_greenlet = _swap_await_only()
    try:
        adapter.close()  # must not raise for a class in the narrow tuple
    finally:
        aio_module.await_only = orig  # type: ignore[attr-defined]

        aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]
    return fake


class TestFinalCloseSuppression:
    def test_operational_error_suppressed(self) -> None:
        from dqlitedbapi.exceptions import OperationalError

        fake = _run_close(OperationalError("op fail", code=None))
        assert fake.close_calls == 1

    def test_interface_error_suppressed(self) -> None:
        from dqlitedbapi.exceptions import InterfaceError

        fake = _run_close(InterfaceError("iface fail"))
        assert fake.close_calls == 1

    def test_dqlite_connection_error_suppressed(self) -> None:
        from dqliteclient.exceptions import DqliteConnectionError

        fake = _run_close(DqliteConnectionError("transport down"))
        assert fake.close_calls == 1

    def test_broken_pipe_os_error_suppressed(self) -> None:
        fake = _run_close(OSError(32, "Broken pipe"))
        assert fake.close_calls == 1

    def test_connection_reset_os_error_suppressed(self) -> None:
        fake = _run_close(ConnectionResetError(104, "reset by peer"))
        assert fake.close_calls == 1

    def test_programmer_bug_propagates(self) -> None:
        """Symmetric regression guard with the terminate() path: a
        programmer bug (AttributeError) must escape so a refactor
        regression doesn't get silently eaten."""
        fake = _FakeAsyncConn(close_exc=AttributeError("typo"))
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
        aio_module, orig, _orig_in_greenlet = _swap_await_only()
        try:
            with pytest.raises(AttributeError):
                adapter.close()
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]

            aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]
        # close() still ran before the programmer bug escaped.
        assert fake.close_calls == 1


class TestCloseMatrix:
    """Cross-check: rollback raising + final-close raising must both
    respect their narrow-catch disciplines. An OSError from rollback
    (first branch) plus an OSError from final-close (finally branch)
    are both suppressed; close() must still complete without raising.
    """

    def test_both_branches_suppress_os_error(self) -> None:
        fake = _FakeAsyncConn(
            close_exc=OSError(32, "Broken pipe"),
            rollback_exc=OSError(104, "reset by peer"),
        )
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
        aio_module, orig, _orig_in_greenlet = _swap_await_only()
        try:
            adapter.close()  # neither branch propagates
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]

            aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]
        assert fake.rollback_calls == 1
        assert fake.close_calls == 1

    def test_operational_rollback_plus_os_final_close(self) -> None:
        from dqlitedbapi.exceptions import OperationalError

        fake = _FakeAsyncConn(
            close_exc=OSError(32, "Broken pipe"),
            rollback_exc=OperationalError("leader flip", code=None),
        )
        adapter = AsyncAdaptedConnection(fake)  # type: ignore[arg-type]
        aio_module, orig, _orig_in_greenlet = _swap_await_only()
        try:
            adapter.close()  # both branches suppress
        finally:
            aio_module.await_only = orig  # type: ignore[attr-defined]

            aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]
        assert fake.rollback_calls == 1
        assert fake.close_calls == 1
