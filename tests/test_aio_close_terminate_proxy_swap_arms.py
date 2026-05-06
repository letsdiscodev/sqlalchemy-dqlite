"""Pin: every exit arm of ``AsyncAdaptedConnection.close()`` and
``AsyncAdaptedConnection.terminate()`` must run the post-close
``weakref.proxy(self._connection)`` swap.

The swap exists specifically because SA's pool diagnostic ring +
pytest session-fixture cache pin closed connections via strong refs;
without the swap, the inner dbapi ``AsyncConnection`` (and through it
the client-layer state, registered ``weakref.finalize``, and any
frame-pinning ``_invalidation_cause``) survives past the user's
intended lifetime.

Two regressions on this discipline:

1. ``close()``'s rollback-arm ``RuntimeError("Event loop is closed")``
   handler ended with an explicit ``return`` BEFORE the post-finally
   swap ran. Same-shape: the ``_handle_exception`` raise arm and the
   close-arm CancelledError raise arm also bypassed the swap.
2. ``terminate()`` had no swap at all — SA's pool invalidate path
   uses terminate, and the same diagnostic-ring / fixture-pinning
   concern applies symmetrically.

The fix wraps both methods' bodies in an outer ``try/finally`` whose
``finally`` calls ``_release_inner_strong_ref`` — the swap runs on
EVERY exit arm (normal return, early return, raise, cancel).

The pins below exercise each early-exit arm and assert the inner is
released. We assert the slot-shape directly (proxy or None) on every
arm and additionally pin the GC-after-del release on no-exception
arms — log records on exception arms hold the rollback / close frame
alive past test-function exit (via ``exc_info=True`` traceback chain),
which would mask the inner-pin regression so we skip the GC check
there. The slot-shape pin alone is sufficient on those arms.
"""

from __future__ import annotations

import gc
import weakref
from typing import Any

import pytest

from sqlalchemydqlite import aio as aio_module
from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _FakeAsyncConn:
    """Minimal inner-conn stub. Supports rollback / close / cursor
    sufficient to run the adapter's ``close()`` and ``terminate()``
    paths under the in-process ``await_only`` shim.
    """

    address = "localhost:9001"
    is_closed = False

    def __init__(
        self,
        rollback_exc: BaseException | None = None,
        close_exc: BaseException | None = None,
    ) -> None:
        self._rollback_exc = rollback_exc
        self._close_exc = close_exc

    async def rollback(self) -> None:
        if self._rollback_exc is not None:
            raise self._rollback_exc

    async def close(self) -> None:
        if self._close_exc is not None:
            raise self._close_exc
        self.is_closed = True

    def cursor(self) -> object:
        return object()


def _fake_await(coro: object) -> object:
    """In-process drop-in for SA's ``await_only`` that drives a stub
    coroutine to completion without a real greenlet context.
    """
    if hasattr(coro, "__await__"):
        it = coro.__await__()
        try:
            next(it)
        except StopIteration as e:
            return e.value
    return None


def _force_close_transport_noop(self: Any) -> None:
    """Replace the writer-bypass force-close so we don't need a real
    inner ``DqliteConnection`` to call into.
    """


def _stub_greenlet_machinery() -> tuple[Any, Any, Any]:
    """Install in-process stubs for the greenlet machinery; return the
    originals so the caller can restore them in a ``finally`` BEFORE
    running the GC-after-del assertions (matches the pattern in
    ``test_aio_close_releases_inner_connection_pin.py``).
    """
    real_await = aio_module.await_only  # type: ignore[attr-defined]
    real_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    real_force_close = AsyncAdaptedConnection._force_close_transport
    aio_module.await_only = _fake_await  # type: ignore[attr-defined,assignment]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    AsyncAdaptedConnection._force_close_transport = _force_close_transport_noop
    return real_await, real_in_greenlet, real_force_close


def _restore_greenlet_machinery(originals: tuple[Any, Any, Any]) -> None:
    real_await, real_in_greenlet, real_force_close = originals
    aio_module.await_only = real_await  # type: ignore[attr-defined]
    aio_module.in_greenlet = real_in_greenlet  # type: ignore[attr-defined]
    AsyncAdaptedConnection._force_close_transport = real_force_close


def _assert_slot_released(adapter: AsyncAdaptedConnection) -> None:
    slot = adapter._connection
    assert slot is None or isinstance(slot, weakref.ProxyTypes), (
        f"adapter._connection must be released after close/terminate "
        f"(weakref.proxy or None); got strong ref of type "
        f"{type(slot).__name__} — the post-close proxy-swap did not "
        f"run on this exit arm"
    )


def test_close_rollback_loop_closed_runs_proxy_swap() -> None:
    """Rollback-arm ``RuntimeError("Event loop is closed")`` was
    handled with an explicit ``return``, bypassing the post-finally
    swap. Pin: this arm now flows through the swap.
    """
    inner = _FakeAsyncConn(rollback_exc=RuntimeError("Event loop is closed"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_rollback_loop_different_runs_proxy_swap() -> None:
    """Symmetric defence on the rollback-arm ``different loop`` /
    ``different event loop`` substrings (routed via
    ``_handle_exception`` to ``OperationalError``). The
    ``_handle_exception`` raise path must release the inner via the
    outer try/finally before propagating.
    """
    import dqlitedbapi.exceptions as _dbapi_exc

    inner = _FakeAsyncConn(rollback_exc=RuntimeError("attached to a different loop"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        with pytest.raises(_dbapi_exc.OperationalError):
            adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_rollback_transport_class_runs_proxy_swap() -> None:
    """Sibling regression-fence: transport-class rollback exception
    falls through to the close-finally + outer-finally swap.
    """
    inner = _FakeAsyncConn(rollback_exc=ConnectionResetError("rollback FIN"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_close_arm_runtime_error_runs_proxy_swap() -> None:
    """Sibling regression-fence: close-arm
    ``RuntimeError("Event loop is closed")`` falls through to the
    outer-finally swap.
    """
    inner = _FakeAsyncConn(close_exc=RuntimeError("Event loop is closed"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_close_arm_transport_class_runs_proxy_swap() -> None:
    """Sibling regression-fence: close-arm transport-class exception
    falls through to the outer-finally swap.
    """
    inner = _FakeAsyncConn(close_exc=ConnectionResetError("close FIN"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_no_exception_path_releases_inner_pin() -> None:
    """No-exception path: belt-and-brace GC-after-del check that the
    proxy swap genuinely releases the inner (not just wraps it in a
    proxy that still pins via cycle). Mirrors the existing
    ``test_aio_close_releases_inner_connection_pin.py`` discipline,
    extended with the new outer try/finally guarantee.
    """
    inner = _FakeAsyncConn()
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)

    _assert_slot_released(adapter)
    inner_ref = weakref.ref(inner)
    inner = None  # type: ignore[assignment]  # drop the test-frame strong ref
    gc.collect()
    assert inner_ref() is None, (
        "no-exception close path must release the inner pin (not just the slot reference)"
    )


def test_terminate_runs_proxy_swap() -> None:
    """Pin: ``terminate()`` must release the inner via proxy-swap on
    its happy path — same diagnostic-ring / fixture-pinning concern
    as ``close()``. SA's pool invalidate path uses terminate; the
    asymmetry between close and terminate is the regression class.
    """
    inner = _FakeAsyncConn()
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.terminate()
    finally:
        _restore_greenlet_machinery(originals)

    _assert_slot_released(adapter)
    inner_ref = weakref.ref(inner)
    inner = None  # type: ignore[assignment]
    gc.collect()
    assert inner_ref() is None, "terminate() must release the inner pin on the no-exception path"


def test_terminate_close_runtime_error_runs_proxy_swap() -> None:
    """``terminate()``'s ``RuntimeError("Event loop is closed")`` arm
    is the typical engine.dispose() shape under per-call
    ``asyncio.run()`` teardown. Pin the swap on this arm too.
    """
    inner = _FakeAsyncConn(close_exc=RuntimeError("Event loop is closed"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.terminate()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_terminate_close_transport_class_runs_proxy_swap() -> None:
    """``terminate()`` transport-class close exception arm — flaky
    close on forced reclaim. Pin the swap on this arm too.
    """
    inner = _FakeAsyncConn(close_exc=ConnectionResetError("terminate FIN"))
    adapter = AsyncAdaptedConnection(inner)  # type: ignore[arg-type]
    originals = _stub_greenlet_machinery()
    try:
        adapter.terminate()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)
