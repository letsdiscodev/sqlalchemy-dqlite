"""close() DEBUG-logs failed rollbacks before continuing; programming bugs
(AttributeError, RuntimeError, etc.) still propagate."""

from __future__ import annotations

import logging

import pytest

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import OperationalError
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


def test_close_logs_rollback_failure(caplog: pytest.LogCaptureFixture) -> None:
    fake = _FakeAsyncConn(OperationalError("server gone"))
    adapter = AsyncAdaptedConnection(fake)

    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        import asyncio

        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig = aio_module.await_only  # type: ignore[attr-defined]
    _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
            adapter.close()
    finally:
        aio_module.await_only = orig  # type: ignore[attr-defined]

        aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]

    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback failed" in r.getMessage()
    ]
    assert matching, f"expected DEBUG 'rollback failed' record; got {caplog.records!r}"
    assert matching[0].exc_info is not None
    assert isinstance(matching[0].exc_info[1], OperationalError)
    assert fake.close_calls == 1
    # Correlation fields id=/peer= let operators attribute the record to a node.
    msg = matching[0].getMessage()
    assert f"id={id(adapter)}" in msg, msg
    assert "peer=" in msg, msg


def test_close_propagates_programming_bug() -> None:
    """RuntimeError / AttributeError / etc. are NOT suppressed."""
    fake = _FakeAsyncConn(RuntimeError("programming bug"))
    adapter = AsyncAdaptedConnection(fake)

    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        import asyncio

        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig = aio_module.await_only  # type: ignore[attr-defined]
    _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        with pytest.raises(RuntimeError, match="programming bug"):
            adapter.close()
    finally:
        aio_module.await_only = orig  # type: ignore[attr-defined]

        aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]


def test_close_with_also_failing_transport_errors(caplog: pytest.LogCaptureFixture) -> None:
    """The full transport-error category is caught and DEBUG-logged."""
    fake = _FakeAsyncConn(DqliteConnectionError("peer reset"))
    adapter = AsyncAdaptedConnection(fake)

    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        import asyncio

        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig = aio_module.await_only  # type: ignore[attr-defined]
    _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
            adapter.close()
    finally:
        aio_module.await_only = orig  # type: ignore[attr-defined]

        aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]

    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback failed" in r.getMessage()
    ]
    assert matching
    assert fake.close_calls == 1


@pytest.mark.parametrize(
    "exc",
    [
        OSError(32, "broken pipe"),
        BrokenPipeError(32, "broken pipe"),
        ConnectionError("peer went away"),
        ConnectionResetError(104, "connection reset by peer"),
        ConnectionAbortedError(103, "software caused connection abort"),
        ConnectionRefusedError(111, "connection refused"),
        TimeoutError("read timed out"),
    ],
)
def test_close_suppresses_os_level_rollback_errors(
    caplog: pytest.LogCaptureFixture, exc: BaseException
) -> None:
    """The single OSError clause must catch every stdlib transport-error
    subclass; narrowing it would leak the AsyncConnection."""
    fake = _FakeAsyncConn(exc)
    adapter = AsyncAdaptedConnection(fake)

    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        import asyncio

        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig = aio_module.await_only  # type: ignore[attr-defined]
    _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
            adapter.close()
    finally:
        aio_module.await_only = orig  # type: ignore[attr-defined]

        aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]

    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback failed" in r.getMessage()
    ]
    assert matching, f"no DEBUG log captured for {type(exc).__name__}: {caplog.records!r}"
    # Log line carries the exception type so operators can correlate the cause.
    assert type(exc).__name__ in matching[0].getMessage()
    assert fake.close_calls == 1


def test_close_propagates_value_error_out_of_tuple() -> None:
    """A ValueError from rollback is outside the narrow tuple and must propagate."""
    fake = _FakeAsyncConn(ValueError("parameter out of range"))
    adapter = AsyncAdaptedConnection(fake)

    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        import asyncio

        return asyncio.new_event_loop().run_until_complete(coro)  # type: ignore[arg-type]

    orig = aio_module.await_only  # type: ignore[attr-defined]
    _orig_in_greenlet = aio_module.in_greenlet  # type: ignore[attr-defined]
    aio_module.await_only = _fake_await_only  # type: ignore[assignment,attr-defined]
    aio_module.in_greenlet = lambda: True  # type: ignore[attr-defined]
    try:
        with pytest.raises(ValueError, match="parameter out of range"):
            adapter.close()
    finally:
        aio_module.await_only = orig  # type: ignore[attr-defined]

        aio_module.in_greenlet = _orig_in_greenlet  # type: ignore[attr-defined]
