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

from sqlalchemy.util import await_only

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _adapter_with_writer() -> tuple[AsyncAdaptedConnection, MagicMock]:
    """Build an adapter with a fake underlying conn whose protocol
    has a writer. Returns (adapter, writer-mock)."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    writer = MagicMock()
    proto = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    adapter._connection = inner
    return adapter, writer


def test_close_outside_greenlet_falls_back_to_sync_writer_close() -> None:
    """``close()`` invoked from a regular sync context (no greenlet)
    must NOT raise MissingGreenlet — the fallback closes the writer
    transport directly."""
    adapter, writer = _adapter_with_writer()
    # Sync-context call: no greenlet provider.
    adapter.close()
    # The sync writer.close() ran in the fallback.
    writer.close.assert_called_once()
    # Local refs cleared so a re-close short-circuits cleanly.
    assert adapter._connection._protocol is None


def test_terminate_outside_greenlet_falls_back_to_sync_writer_close() -> None:
    adapter, writer = _adapter_with_writer()
    adapter.terminate()
    writer.close.assert_called_once()
    assert adapter._connection._protocol is None


def test_force_close_transport_idempotent_with_no_protocol() -> None:
    """If the protocol is already None (torn down or never opened),
    the fallback is a clean no-op."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner._protocol = None
    inner.address = "localhost:9001"
    adapter._connection = inner
    # Should not raise.
    adapter._force_close_transport()


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
