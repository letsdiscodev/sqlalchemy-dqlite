"""Pin: ``AsyncAdaptedConnection.close()`` and ``terminate()`` use
``in_greenlet()`` preflight to short-circuit to the synchronous
force-close path WITHOUT invoking ``await_only`` (which would
allocate a ``MissingGreenlet`` exception with full traceback only
to be caught and absorbed).

Matches the SA reference adapter pattern in
``sqlalchemy/connectors/asyncio.py``: ``if in_greenlet():
self.await_(...) else: self._terminate_force_close()``.

Behavioural deltas (preflight vs. exception-dispatch):
1. No ``MissingGreenlet`` allocation on every non-greenlet close.
2. Idiom alignment with SA reference.
3. Three-arm collapse — only ``RuntimeError`` and ``CancelledError``
   handling stays in the post-await arms; ``MissingGreenlet`` arms
   are removed.

Functionally equivalent today; this is a parity-with-SA-reference
cleanup with a small perf delta.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class TestCloseWithoutGreenletShortCircuitsToForceClose:
    def test_close_outside_greenlet_does_not_call_await_only(self) -> None:
        """Outside a greenlet context, ``close()`` must skip
        ``await_only`` entirely and call ``_force_close_transport``
        directly. If ``await_only`` were reached, it would raise
        ``MissingGreenlet``; the preflight should make that
        unreachable."""
        raw = MagicMock()
        adapted = AsyncAdaptedConnection(raw)

        with (
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
            patch("sqlalchemydqlite.aio.await_only") as await_only_mock,
        ):
            # Tests run outside an SA greenlet, so in_greenlet()
            # is False here. The preflight should short-circuit.
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
