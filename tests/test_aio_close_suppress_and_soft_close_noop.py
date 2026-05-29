"""Two SA-aio pins: connect()'s cleanup suppresses close-time exceptions so the
original connect failure propagates; _async_soft_close is a deliberate no-op."""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock, patch

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _run_sync(coro_or_value: object) -> object:
    """await_only replacement that resolves coroutines synchronously."""
    if hasattr(coro_or_value, "__await__") or hasattr(coro_or_value, "cr_await"):
        try:
            coro_or_value.send(None)  # type: ignore[attr-defined]
        except StopIteration as e:
            return e.value
    return coro_or_value


class TestAioConnectClosesRawConnAndSuppressesCloseException:
    """Eager-connect cleanup force-closes the raw conn, suppresses any
    terminate error, and re-raises the original connect-time exception."""

    def test_close_exception_is_suppressed_when_eager_connect_fails(self) -> None:
        """The original connect-time exception propagates even when cleanup raises."""
        from dqliteclient.exceptions import DqliteConnectionError
        from sqlalchemydqlite.aio import DqliteDialect_aio

        dialect = DqliteDialect_aio()

        raw_conn = MagicMock()
        raw_conn.connect = MagicMock(side_effect=DqliteConnectionError("eager failed"))
        # Raise from the terminate fallback to prove the connect error still wins.
        raw_conn.force_close_transport = MagicMock(side_effect=RuntimeError("force-close raised"))

        dbapi = MagicMock()
        dbapi.connect = MagicMock(return_value=raw_conn)
        dialect.loaded_dbapi = dbapi

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=_run_sync),
            pytest.raises(DqliteConnectionError, match="eager failed"),
        ):
            dialect.connect()

        raw_conn.force_close_transport.assert_called()


class TestAsyncAdaptedCursorSoftCloseNoop:
    """_async_soft_close is a deliberate no-op: the result set is already
    materialised into an in-memory deque, so there are no handles to close."""

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

        prior_method_calls = list(underlying.method_calls)

        await cur._async_soft_close()

        assert list(underlying.method_calls) == prior_method_calls, (
            "_async_soft_close must not invoke any method on the underlying connection"
        )

        # Cursor state unchanged — soft-close is documentary, not destructive.
        assert cur._rows == deque([(1,), (2,)])
        assert cur.rowcount == 2
        assert cur.description == (("col", None, None, None, None, None, None),)
        assert cur._closed is False
