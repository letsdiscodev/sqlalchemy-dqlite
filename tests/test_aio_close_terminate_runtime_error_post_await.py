"""Pin: ``AsyncAdaptedConnection.close()`` and ``terminate()`` reap the
writer transport synchronously when ``await_only`` raises
``RuntimeError("Event loop is closed")`` post-await.

A canonical scenario: the SA engine is built and used inside a
per-call ``asyncio.run()`` block. When the block exits the loop is
torn down; SA's pool finalizer then calls ``terminate()`` /
``close()``. ``await_only`` cannot drive the async machinery on a
defunct loop and raises ``RuntimeError("Event loop is closed")``.

Without ``self._force_close_transport()`` in the
``except RuntimeError`` arm, the writer transport leaks until GC.
``has_terminate=True`` (the dialect-level promise) means dispose
must not propagate failures from this path either, so the call must
also stay quiet (DEBUG log only).

Existing coverage:
- ``test_aio_close_terminate_in_greenlet_preflight.py`` covers the
  no-greenlet short-circuit (``await_only`` is never called).
- ``test_aio_close_suppress_and_soft_close_noop.py`` exercises the
  rollback-side RuntimeError "different loop" remap arm.

The post-``await_only`` RuntimeError arms in close()'s ``finally``
and in terminate() were untested.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


@pytest.fixture
def fake_greenlet():
    """Force ``in_greenlet()`` True so the preflight does not
    short-circuit and the await arms are entered."""
    with patch("sqlalchemydqlite.aio.in_greenlet", return_value=True):
        yield


class TestCloseRuntimeErrorPostAwaitReapsTransport:
    def test_close_runtime_error_event_loop_closed_calls_force_close(
        self, fake_greenlet: None
    ) -> None:
        """``await_only`` raises RuntimeError("Event loop is closed")
        from the close arm; ``_force_close_transport`` must run and
        nothing must propagate."""
        raw = MagicMock()
        # Make the rollback await succeed (returns immediately) and
        # the close await raise RuntimeError. The first await_only
        # call is for rollback, the second is for close.
        rollback_coro = MagicMock()
        close_coro = MagicMock()
        raw.rollback = MagicMock(return_value=rollback_coro)
        raw.close = MagicMock(return_value=close_coro)

        adapted = AsyncAdaptedConnection(raw)

        call_count = {"n": 0}

        def fake_await_only(coro: object) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                # rollback succeeds
                return None
            # close raises RuntimeError("Event loop is closed")
            raise RuntimeError("Event loop is closed")

        with (
            patch("sqlalchemydqlite.aio.await_only", side_effect=fake_await_only),
            patch.object(AsyncAdaptedConnection, "_force_close_transport") as fct,
        ):
            # Must not raise
            adapted.close()

        fct.assert_called_once()

    def test_close_runtime_error_different_loop_calls_force_close(
        self, fake_greenlet: None
    ) -> None:
        """Sibling msg variant — RuntimeError("attached to a different
        loop") from the close arm reaches the same RuntimeError
        catch and reaps the transport."""
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
        """``terminate()`` skips rollback, so the very first
        ``await_only`` call is the close — RuntimeError there must
        reap the transport synchronously and stay quiet."""
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
