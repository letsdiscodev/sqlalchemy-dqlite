"""Two SA-aio test pins:

1. ``DqliteDialect_aio.connect()``'s ``contextlib.suppress(Exception)``
   wrap around ``raw_conn.close()`` after a failed eager-connect
   actually swallows close-time exceptions, leaving the original
   connect-time exception to propagate. Existing tests cover the
   close-succeeds case but never set ``raw_conn.close.side_effect``
   so the suppression contract was unverified.

2. ``AsyncAdaptedCursor._async_soft_close`` is a deliberate no-op.
   Currently only integration-tested (live cluster required); pin
   the unit-level no-op behaviour so the contract has a fast-feedback
   guard and the cluster dependency on this contract can be dropped.
"""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock, patch

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _run_sync(coro_or_value: object) -> object:
    """Replacement for await_only that resolves coroutines synchronously."""
    if hasattr(coro_or_value, "__await__") or hasattr(coro_or_value, "cr_await"):
        try:
            coro_or_value.send(None)  # type: ignore[attr-defined]
        except StopIteration as e:
            return e.value
    return coro_or_value


class TestAioConnectClosesRawConnAndSuppressesCloseException:
    """The eager-connect cleanup path must:

    1. Run the SA-adapter ``terminate()`` shape (force-close, no
       rollback) on the raw connection — the connection's handshake
       never completed so a graceful close has nothing to drain.
    2. SUPPRESS any exception raised by terminate — without this, a
       broken terminate would supplant the original connect failure.
    3. Re-raise the ORIGINAL connect-time exception unchanged.
    """

    def test_close_exception_is_suppressed_when_eager_connect_fails(self) -> None:
        """The original connect-time exception must propagate even
        when cleanup-time raises (terminate's force-close path may
        fail on a defunct loop after a per-call ``asyncio.run()``)."""
        from dqliteclient.exceptions import DqliteConnectionError
        from sqlalchemydqlite.aio import DqliteDialect_aio

        dialect = DqliteDialect_aio()

        raw_conn = MagicMock()
        raw_conn.connect = MagicMock(side_effect=DqliteConnectionError("eager failed"))
        # force_close_transport is the terminate fallback path used
        # outside a greenlet; raise from it to prove the original
        # connect-time exception still wins.
        raw_conn.force_close_transport = MagicMock(side_effect=RuntimeError("force-close raised"))

        dbapi = MagicMock()
        dbapi.connect = MagicMock(return_value=raw_conn)
        dialect.loaded_dbapi = dbapi

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(DqliteConnectionError, match="eager failed"),
        ):
            dialect.connect()

        # Cleanup ran via terminate's force-close path even though
        # it raised; the original exception still propagated.
        raw_conn.force_close_transport.assert_called()


class TestAsyncAdaptedCursorSoftCloseNoop:
    """``AsyncAdaptedCursor._async_soft_close`` is deliberately a
    no-op: the SA Result layer calls it during result-finalization
    teardown to release any per-cursor resources, but
    ``AsyncAdaptedCursor`` materialises the entire result set into
    its in-memory deque at execute-time, so there are no underlying
    cursor handles to close. Pin the no-op contract so a future
    refactor that adds work to ``_async_soft_close`` (or removes it)
    is caught at unit level."""

    @pytest.mark.asyncio
    async def test_async_soft_close_is_a_noop(self) -> None:
        adapted_conn = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        underlying = MagicMock()
        adapted_conn._connection = underlying
        cur = AsyncAdaptedCursor(adapted_conn)
        cur._rows = deque([(1,), (2,)])
        cur.description = (("col", None, None, None, None, None, None),)
        cur.rowcount = 2
        cur.lastrowid = None

        # Snapshot the underlying connection's call list before.
        prior_method_calls = list(underlying.method_calls)

        await cur._async_soft_close()

        # No interactions with the underlying connection or its
        # methods.
        assert list(underlying.method_calls) == prior_method_calls, (
            "_async_soft_close must not invoke any method on the underlying connection"
        )

        # Cursor state remains unchanged — soft-close is documentary,
        # not destructive.
        assert cur._rows == deque([(1,), (2,)])
        assert cur.rowcount == 2
        assert cur.description == (("col", None, None, None, None, None, None),)

        # Cursor must NOT have been close()'d as a side-effect.
        assert cur._closed is False
