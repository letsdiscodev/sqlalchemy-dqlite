"""AsyncAdaptedConnection.close() must run the underlying close() even if
rollback() raises outside the narrow suppression tuple.

SQLAlchemy's async pool calls close() during dispose / teardown. If
rollback() raises an exception not in the narrow catch tuple
(``BaseException`` like ``CancelledError``, programming bugs, etc.),
control previously skipped the final ``await_only(self._connection.close())``
line and the underlying AsyncConnection leaked. SA's pool does NOT
re-call close() on failure, so the leak is permanent.

Wrap rollback in ``try/finally`` so close() runs on any rollback
outcome. Mirror of the inverse leak guarded by the connect() path.
"""

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
    """Replace await_only in the aio module with a sync driver so tests
    don't need a live SA engine / greenlet context.

    Also patch ``in_greenlet`` to return True so the preflight in
    ``close()`` / ``terminate()`` doesn't short-circuit to the
    sync force-close path — these tests are exercising the
    in-greenlet branch.
    """
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
    """CancelledError is a BaseException and not in the narrow catch
    tuple. The underlying close() must still run — and then the
    CancelledError propagates as the active exception.
    """
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
    """A programming bug in rollback (AttributeError) still must not
    prevent close() from running, even though the bug propagates.
    """
    fake = _FakeAsyncConn(AttributeError("bug"))
    adapter = AsyncAdaptedConnection(fake)

    aio_module, orig_await, orig_in_greenlet = _install_fake_await_only()
    try:
        with pytest.raises(AttributeError, match="bug"):
            adapter.close()
    finally:
        _restore_await_only(aio_module, orig_await, orig_in_greenlet)

    assert fake.close_calls == 1
