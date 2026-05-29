"""``_terminate_handled_exceptions`` must advertise ``RuntimeError`` (defunct-loop
close shape) alongside the transport tuple and ``CancelledError``, matching the
three catch arms in the hand-rolled ``terminate()`` body."""

from __future__ import annotations

import asyncio

from sqlalchemydqlite.aio import AsyncAdaptedConnection
from sqlalchemydqlite.base import _TRANSPORT_CLASS_EXCEPTIONS


def test_terminate_handled_exceptions_includes_runtime_error() -> None:
    handled = AsyncAdaptedConnection._terminate_handled_exceptions()
    assert RuntimeError in handled, (
        "terminate() body has a RuntimeError catch arm (defunct-loop "
        "close shape); the introspection tuple must advertise it. "
        f"Got: {handled}"
    )


def test_terminate_handled_exceptions_includes_transport_and_cancel() -> None:
    handled = AsyncAdaptedConnection._terminate_handled_exceptions()
    for cls in _TRANSPORT_CLASS_EXCEPTIONS:
        assert cls in handled, f"{cls.__name__} missing from terminate-handled tuple"
    assert asyncio.CancelledError in handled
