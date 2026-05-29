"""close() and terminate() reap the writer transport synchronously and stay
quiet (DEBUG only) when await_only raises RuntimeError("Event loop is closed")
post-await, e.g. SA's pool finalizing on a loop torn down by asyncio.run()."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


@pytest.fixture
def fake_greenlet():
    """Force in_greenlet() True so the await arms are entered."""
    with patch("sqlalchemydqlite.aio.in_greenlet", return_value=True):
        yield


class TestCloseRuntimeErrorPostAwaitReapsTransport:
    def test_close_runtime_error_event_loop_closed_calls_force_close(
        self, fake_greenlet: None
    ) -> None:
        """Close-arm RuntimeError("Event loop is closed") runs
        _force_close_transport and propagates nothing."""
        raw = MagicMock()
        # First await_only call is rollback (succeeds), second is close (raises).
        rollback_coro = MagicMock()
        close_coro = MagicMock()
        raw.rollback = MagicMock(return_value=rollback_coro)
        raw.close = MagicMock(return_value=close_coro)

        adapted = AsyncAdaptedConnection(raw)

        call_count = {"n": 0}

        def fake_await_only(coro: object) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return None
            raise RuntimeError("Event loop is closed")

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=fake_await_only),
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
        ):
            adapted.close()  # must not raise

        fct.assert_called_once()

    def test_close_runtime_error_different_loop_calls_force_close(
        self, fake_greenlet: None
    ) -> None:
        """RuntimeError("attached to a different loop") reaches the same catch."""
        raw = MagicMock()
        raw.rollback = MagicMock(return_value=MagicMock())
        raw.close = MagicMock(return_value=MagicMock())

        adapted = AsyncAdaptedConnection(raw)
        call_count = {"n": 0}

        def fake_await_only(coro: object) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return None
            raise RuntimeError("Future attached to a different loop")

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=fake_await_only),
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
        ):
            adapted.close()

        fct.assert_called_once()


class TestTerminateRuntimeErrorPostAwaitReapsTransport:
    def test_terminate_runtime_error_event_loop_closed_calls_force_close(
        self, fake_greenlet: None
    ) -> None:
        """terminate() skips rollback, so the first await_only is the close —
        RuntimeError there reaps the transport and stays quiet."""
        raw = MagicMock()
        raw.close = MagicMock(return_value=MagicMock())
        adapted = AsyncAdaptedConnection(raw)

        with (
            patch(
                "sqlalchemydqlite.aio.await_only",
                side_effect=RuntimeError("Event loop is closed"),
            ),
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
        ):
            adapted.terminate()

        fct.assert_called_once()
