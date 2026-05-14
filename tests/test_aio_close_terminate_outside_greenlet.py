"""Pin: ``AsyncAdaptedConnection.close()`` and ``.terminate()`` fall
back to a synchronous transport close when invoked outside a
greenlet context.

Without the fallback, ``await_only(...)`` raises
``sqlalchemy.exc.MissingGreenlet`` from non-greenlet finalize paths
(GC sweep, atexit, background sync threads). SA's pool's
``_close_connection`` catches ``BaseException``, so the
``MissingGreenlet`` is silently absorbed — and the underlying
``AsyncConnection`` (and its socket) leak until process exit.

The fix invokes ``_force_close_transport()`` which closes the writer
synchronously (no event loop / greenlet needed). Idempotent: if
there's no protocol / writer (already torn down or never opened),
it's a no-op.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.util import await_only

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _adapter_with_writer() -> tuple[AsyncAdaptedConnection, MagicMock]:
    """Build an adapter with a fake underlying dbapi connection whose
    public ``force_close_transport`` hook closes a writer mock.
    Returns (adapter, writer-mock).

    Models the real chain: the SA adapter owns a dbapi
    ``AsyncConnection`` (``self._connection``), which exposes the
    public hook ``force_close_transport``. The hook walks
    ``self._async_conn._protocol._writer.close()`` internally; we
    just need the adapter to call the hook for this test, so wire
    the writer-close into the hook directly.
    """
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    writer = MagicMock()
    inner.force_close_transport = MagicMock(side_effect=writer.close)
    adapter._connection = inner
    return adapter, writer


def test_close_outside_greenlet_falls_back_to_sync_writer_close() -> None:
    """``close()`` invoked from a regular sync context (no greenlet)
    must NOT raise MissingGreenlet — the fallback delegates to the
    dbapi public ``force_close_transport`` hook, which closes the
    writer transport directly."""
    adapter, writer = _adapter_with_writer()
    # Sync-context call: no greenlet provider.
    adapter.close()
    # The sync writer.close() ran in the fallback (via the dbapi hook).
    writer.close.assert_called_once()
    # The dbapi hook itself was invoked.
    adapter._connection.force_close_transport.assert_called_once()


def test_terminate_outside_greenlet_falls_back_to_sync_writer_close() -> None:
    adapter, writer = _adapter_with_writer()
    adapter.terminate()
    writer.close.assert_called_once()
    adapter._connection.force_close_transport.assert_called_once()


def test_force_close_transport_idempotent_with_no_protocol() -> None:
    """If the protocol is already None (torn down or never opened),
    the fallback is a clean no-op. The dbapi hook itself walks the
    null-check internally; we simply call through."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.force_close_transport = MagicMock()  # idempotent dbapi hook
    adapter._connection = inner
    # Should not raise.
    adapter._force_close_transport()
    inner.force_close_transport.assert_called_once()


def test_force_close_transport_falls_back_when_dbapi_hook_missing() -> None:
    """Older dbapi versions may not expose ``force_close_transport``;
    the SA adapter must absorb that case (idempotent, never-raises)
    rather than crash. The cost is a silent FD leak — release notes
    must highlight the dbapi-version requirement.
    """
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    # ``spec=[]`` denies every auto-generated attribute; ``hasattr`` /
    # ``getattr(..., default)`` returns the default. Models a dbapi
    # built before ``force_close_transport`` existed.
    inner = MagicMock(spec=["address"])
    inner.address = "localhost:9001"
    adapter._connection = inner
    # Must not raise even though no hook exists.
    adapter._force_close_transport()


def test_force_close_transport_missing_hook_logs_teardown_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The missing-hook arm logs ``"transport teardown skipped"`` —
    a single substring operators can grep when an older dbapi is
    preventing FD reclamation. Orthogonal to the no-raise pin above:
    that one guards the contract that the arm absorbs the missing
    hook; this one guards the audit trail that records the no-op."""
    import logging

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    # ``spec=["address"]`` denies every other auto-generated
    # attribute — models a dbapi built before
    # ``force_close_transport`` existed.
    inner = MagicMock(spec=["address"])
    inner.address = "localhost:9001"
    adapter._connection = inner
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.aio")
    # Must not raise even though no hook exists.
    adapter._force_close_transport()
    assert any("transport teardown skipped" in record.getMessage() for record in caplog.records), (
        f"missing 'transport teardown skipped' substring; "
        f"got: {[r.getMessage() for r in caplog.records]}"
    )


def test_force_close_transport_logs_sync_fallback_substring(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fallback's debug log says "sync fallback" — a single
    substring that operators can grep for, regardless of which of
    the three caller paths (MissingGreenlet, RuntimeError,
    CancelledError) reached the fallback."""
    import logging

    adapter, _ = _adapter_with_writer()
    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.aio")
    adapter._force_close_transport()
    assert any("sync fallback" in record.getMessage() for record in caplog.records), (
        f"missing 'sync fallback' substring; got: {[r.getMessage() for r in caplog.records]}"
    )


def test_close_inside_greenlet_uses_normal_async_path() -> None:
    """Negative pin: from a greenlet context, ``await_only`` works
    and the sync fallback is NOT invoked. Verify by spying on the
    fallback method via the writer NOT being called (the async
    underlying ``close()`` is what runs)."""
    import asyncio

    from sqlalchemy.util import greenlet_spawn

    adapter, writer = _adapter_with_writer()

    async def underlying_close() -> None:
        # Simulate the async close — does NOT touch the writer
        # directly (the real close goes through the protocol's own
        # close machinery).
        return None

    adapter._connection.close = underlying_close

    async def run() -> None:
        await greenlet_spawn(adapter.terminate)

    asyncio.run(run())
    # The sync fallback's writer.close was NOT called.
    writer.close.assert_not_called()


# Sanity: importing await_only at module level so the test file
# doesn't accidentally bypass the SA stack we're integrating with.
_ = await_only
