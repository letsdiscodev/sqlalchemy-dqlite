"""Every exit arm of close() and terminate() must run the post-close
weakref.proxy swap, releasing the inner dbapi AsyncConnection that SA's pool
ring / pytest fixture cache would otherwise pin via strong refs.

Exception arms assert the slot shape only: their log records hold the frame
alive via exc_info traceback, which would mask a GC-after-del check.
"""

from __future__ import annotations

import gc
import weakref
from typing import Any

import pytest

from sqlalchemydqlite import aio as aio_module
from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _FakeAsyncConn:
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
    """In-process drop-in for await_only, no greenlet context needed."""
    if hasattr(coro, "__await__"):
        it = coro.__await__()
        try:
            next(it)
        except StopIteration as e:
            return e.value
    return None


def _force_close_transport_noop(self: Any) -> None:
    """No-op so we don't need a real inner DqliteConnection."""


def _stub_greenlet_machinery() -> tuple[Any, Any, Any]:
    """Install in-process greenlet-machinery stubs; return originals to restore
    in a finally before the GC-after-del assertions run."""
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
    """Rollback-arm RuntimeError("Event loop is closed") flows through the swap
    (it previously returned early, bypassing it)."""
    inner = _FakeAsyncConn(rollback_exc=RuntimeError("Event loop is closed"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_rollback_loop_different_runs_proxy_swap() -> None:
    """The _handle_exception raise path (rollback "different loop") must release
    the inner via the outer try/finally before propagating."""
    import dqlitedbapi.exceptions as _dbapi_exc

    inner = _FakeAsyncConn(rollback_exc=RuntimeError("attached to a different loop"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        with pytest.raises(_dbapi_exc.OperationalError):
            adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_rollback_transport_class_runs_proxy_swap() -> None:
    """Transport-class rollback exception falls through to the swap."""
    inner = _FakeAsyncConn(rollback_exc=ConnectionResetError("rollback FIN"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_close_arm_runtime_error_runs_proxy_swap() -> None:
    """Close-arm RuntimeError("Event loop is closed") falls through to the swap."""
    inner = _FakeAsyncConn(close_exc=RuntimeError("Event loop is closed"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_close_arm_transport_class_runs_proxy_swap() -> None:
    """Close-arm transport-class exception falls through to the swap."""
    inner = _FakeAsyncConn(close_exc=ConnectionResetError("close FIN"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_close_no_exception_path_releases_inner_pin() -> None:
    """No-exception path: GC-after-del check that the swap genuinely releases the
    inner, not just wraps it in a still-pinning proxy."""
    inner = _FakeAsyncConn()
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.close()
    finally:
        _restore_greenlet_machinery(originals)

    _assert_slot_released(adapter)
    inner_ref = weakref.ref(inner)
    inner = None  # type: ignore[assignment]
    gc.collect()
    assert inner_ref() is None, (
        "no-exception close path must release the inner pin (not just the slot reference)"
    )


def test_terminate_runs_proxy_swap() -> None:
    """terminate() releases the inner via proxy-swap on its happy path; SA's pool
    invalidate path uses terminate, so the close/terminate asymmetry is the risk."""
    inner = _FakeAsyncConn()
    adapter = AsyncAdaptedConnection(inner)
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
    """terminate()'s RuntimeError("Event loop is closed") arm (the typical
    engine.dispose() shape under per-call asyncio.run()) runs the swap."""
    inner = _FakeAsyncConn(close_exc=RuntimeError("Event loop is closed"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.terminate()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)


def test_terminate_close_transport_class_runs_proxy_swap() -> None:
    """terminate() transport-class close exception arm runs the swap."""
    inner = _FakeAsyncConn(close_exc=ConnectionResetError("terminate FIN"))
    adapter = AsyncAdaptedConnection(inner)
    originals = _stub_greenlet_machinery()
    try:
        adapter.terminate()
    finally:
        _restore_greenlet_machinery(originals)
    _assert_slot_released(adapter)
