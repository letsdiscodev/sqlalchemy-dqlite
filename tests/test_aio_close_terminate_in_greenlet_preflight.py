"""close() and terminate() use an in_greenlet() preflight to short-circuit to
the sync force-close path without invoking await_only (which would allocate a
MissingGreenlet only to be absorbed). Matches SA's reference adapter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class TestCloseWithoutGreenletShortCircuitsToForceClose:
    def test_close_outside_greenlet_does_not_call_await_only(self) -> None:
        """Outside a greenlet, close() skips await_only and calls
        _force_close_transport directly (await_only would raise MissingGreenlet)."""
        raw = MagicMock()
        adapted = AsyncAdaptedConnection(raw)

        with (
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
            patch("sqlalchemydqlite.aio.await_only") as await_only_mock,
        ):
            # Tests run outside a greenlet, so the preflight short-circuits.
            adapted.close()

            await_only_mock.assert_not_called()
            fct.assert_called_once()


class TestTerminateWithoutGreenletShortCircuitsToForceClose:
    def test_terminate_outside_greenlet_does_not_call_await_only(self) -> None:
        raw = MagicMock()
        adapted = AsyncAdaptedConnection(raw)

        with (
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
            patch("sqlalchemydqlite.aio.await_only") as await_only_mock,
        ):
            adapted.terminate()

            await_only_mock.assert_not_called()
            fct.assert_called_once()
