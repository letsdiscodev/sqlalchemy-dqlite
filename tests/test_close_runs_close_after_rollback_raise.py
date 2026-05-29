"""AsyncAdaptedConnection.close() runs the underlying close() even if
rollback() raises outside the narrow suppression tuple. SA's pool does
not re-call close() on failure, so a skipped close leaks permanently."""

from __future__ import annotations

import asyncio

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _FakeAsyncConn:
    def __init__(self, rollback_exc: BaseException | None) -> None:
        self._rollback_exc = rollback_exc
        self.close_calls = 0

    async def rollback(self) -> None:
        if self._rollback_exc is not None:
            raise self._rollback_exc

    async def close(self) -> None:
        self.close_calls += 1


def _install_fake_await_only() -> tuple[object, object, object]:
    """Sync-drive await_only (no live engine) and force in_greenlet=True
    so close()/terminate() exercise the in-greenlet branch."""
    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig_await = aio_module.await_only  # type: ignore[attr-defined]
    orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    return aio_module, orig_await, orig_in_greenlet


def _restore_await_only(aio_module: object, orig_await: object, orig_in_greenlet: object) -> None:
    aio_module.await_only = orig_await  # type: ignore[attr-defined]
    aio_module.in_greenlet = orig_in_greenlet  # type: ignore[attr-defined]


def test_close_runs_after_rollback_raises_cancelled_error() -> None:
    """CancelledError (BaseException, not in the catch tuple) still runs
    close(), then propagates."""
    fake = _FakeAsyncConn(asyncio.CancelledError())
    adapter = AsyncAdaptedConnection(fake)

    aio_module, orig_await, orig_in_greenlet = _install_fake_await_only()
    try:
        with pytest.raises(asyncio.CancelledError):
            adapter.close()
    finally:
        _restore_await_only(aio_module, orig_await, orig_in_greenlet)

    assert fake.close_calls == 1


def test_close_runs_after_rollback_raises_attribute_error() -> None:
    """An AttributeError in rollback still runs close(), then propagates."""
    fake = _FakeAsyncConn(AttributeError("bug"))
    adapter = AsyncAdaptedConnection(fake)

    aio_module, orig_await, orig_in_greenlet = _install_fake_await_only()
    try:
        with pytest.raises(AttributeError, match="bug"):
            adapter.close()
    finally:
        _restore_await_only(aio_module, orig_await, orig_in_greenlet)

    assert fake.close_calls == 1
