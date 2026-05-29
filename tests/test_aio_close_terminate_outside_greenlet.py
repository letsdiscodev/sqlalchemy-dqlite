"""close() and terminate() fall back to a sync transport close outside a
greenlet, where await_only would raise MissingGreenlet — silently absorbed by
SA's pool, leaking the AsyncConnection and its socket until process exit."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.util import await_only

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _adapter_with_writer() -> tuple[AsyncAdaptedConnection, MagicMock]:
    """Adapter whose inner force_close_transport hook closes a writer mock.

    Returns (adapter, writer-mock).
    """
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    writer = MagicMock()
    inner.force_close_transport = MagicMock(side_effect=writer.close)
    adapter._connection = inner
    return adapter, writer


def test_close_outside_greenlet_falls_back_to_sync_writer_close() -> None:
    """close() from a sync context must not raise MissingGreenlet; it delegates
    to the dbapi force_close_transport hook."""
    adapter, writer = _adapter_with_writer()
    adapter.close()
    writer.close.assert_called_once()
    adapter._connection.force_close_transport.assert_called_once()


def test_terminate_outside_greenlet_falls_back_to_sync_writer_close() -> None:
    adapter, writer = _adapter_with_writer()
    adapter.terminate()
    writer.close.assert_called_once()
    adapter._connection.force_close_transport.assert_called_once()


def test_force_close_transport_idempotent_with_no_protocol() -> None:
    """If the protocol is already None, the fallback is a clean no-op."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.force_close_transport = MagicMock()
    adapter._connection = inner
    adapter._force_close_transport()  # must not raise
    inner.force_close_transport.assert_called_once()


def test_force_close_transport_falls_back_when_dbapi_hook_missing() -> None:
    """An older dbapi without force_close_transport must be absorbed (never
    raises); the cost is a silent FD leak flagged in release notes."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    # spec=["address"] models a dbapi built before force_close_transport existed.
    inner = MagicMock(spec=["address"])
    inner.address = "localhost:9001"
    adapter._connection = inner
    adapter._force_close_transport()  # must not raise


def test_force_close_transport_missing_hook_logs_teardown_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The missing-hook arm logs "transport teardown skipped" for operators to
    grep when an older dbapi prevents FD reclamation."""
    import logging

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    # spec=["address"] models a dbapi built before force_close_transport existed.
    inner = MagicMock(spec=["address"])
    inner.address = "localhost:9001"
    adapter._connection = inner
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.aio")
    adapter._force_close_transport()  # must not raise
    assert any("transport teardown skipped" in record.getMessage() for record in caplog.records), (
        f"missing 'transport teardown skipped' substring; "
        f"got: {[r.getMessage() for r in caplog.records]}"
    )


def test_force_close_transport_logs_sync_fallback_substring(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fallback's debug log says "sync fallback" so operators can grep it
    regardless of which caller path reached it."""
    import logging

    adapter, _ = _adapter_with_writer()
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.aio")
    adapter._force_close_transport()
    assert any("sync fallback" in record.getMessage() for record in caplog.records), (
        f"missing 'sync fallback' substring; got: {[r.getMessage() for r in caplog.records]}"
    )


def test_close_inside_greenlet_uses_normal_async_path() -> None:
    """From a greenlet context, await_only works and the sync fallback (writer
    close) is NOT invoked."""
    import asyncio

    from sqlalchemy.util import greenlet_spawn

    adapter, writer = _adapter_with_writer()

    async def underlying_close() -> None:
        return None

    adapter._connection.close = underlying_close

    async def run() -> None:
        await greenlet_spawn(adapter.terminate)

    asyncio.run(run())
    writer.close.assert_not_called()


# Reference await_only so the import isn't flagged as unused.
_ = await_only
