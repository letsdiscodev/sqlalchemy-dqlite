"""Pin the DEBUG-log breadcrumb on AsyncAdaptedConnection.close.

The narrow ``contextlib.suppress`` used to hide failed rollbacks
(leader flip, transport timeout, etc.) silently. Replace with a
try/except that DEBUG-logs before continuing to the close. Programming
bugs (AttributeError, RuntimeError, etc.) still propagate.
"""

from __future__ import annotations

import logging

import pytest

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _FakeAsyncConn:
    """A synchronous stand-in: sqlalchemy.util.await_only accepts a plain
    callable's return when the adapter is driven by greenlet glue. For
    testing we short-circuit to a sync method and let ``await_only``
    see the already-complete coroutine-ish value.
    """

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

    # sqlalchemy.util.await_only drives the coroutine; using a live
    # engine is overkill for an observability test. Stub await_only.
    from sqlalchemydqlite import aio as aio_module

    def _fake_await_only(coro: object) -> object:
        # ``coro`` is a coroutine object; run it synchronously via send.
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
    assert fake.close_calls == 1  # close still ran
    # Correlation fields: id=<num> and peer=<addr-or-None> so operators
    # can attribute the log record to a specific adapter instance / node.
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
    """The full category tuple (OSError, TimeoutError, ConnectionError, etc.)
    is all caught and DEBUG-logged.
    """
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
    """The narrow suppression tuple on ``close()`` catches every
    stdlib transport-error shape via the single ``OSError`` entry.
    ``ConnectionError``, ``BrokenPipeError``, ``ConnectionResetError``,
    ``ConnectionAbortedError``, ``ConnectionRefusedError``, and
    ``TimeoutError`` are all ``OSError`` subclasses since Python
    3.3/3.10, so enumerating them separately would be redundant —
    parametrize the full family so a refactor that narrows the
    ``OSError`` clause (e.g. back to a subset like ``BrokenPipeError``)
    is caught by at least one of these branches rather than silently
    re-raising and leaking the underlying AsyncConnection.
    """
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
    # Log line carries the exception type (so operators triaging a
    # noisy pool can correlate the cause without enabling exc_info
    # rendering). ``%s`` with ``type(exc).__name__`` in the format.
    assert type(exc).__name__ in matching[0].getMessage()
    # close() ran regardless of which transport error fired the
    # suppression branch.
    assert fake.close_calls == 1


def test_close_propagates_value_error_out_of_tuple() -> None:
    """Inverse pin: a ``ValueError`` from rollback is not in the
    narrow tuple and must propagate. Guards against a refactor
    widening the suppression back to ``except Exception``.
    """
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
