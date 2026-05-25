"""Pin: ``AsyncAdaptedConnection._terminate_handled_exceptions``
advertises ``RuntimeError`` alongside the transport-class tuple and
``asyncio.CancelledError``.

The hand-rolled ``terminate()`` body catches three arms — the
project-wide ``_TRANSPORT_CLASS_EXCEPTIONS`` tuple, ``RuntimeError``
(defunct-loop close shape), and ``asyncio.CancelledError`` — but the
introspection helper previously returned only the transport tuple +
CancelledError, under-reporting by one class. Third-party SA async
tooling (Sentry async-pool wrapper, sqlalchemy-utils diagnostics)
that mirrored the body's catch surface via this tuple treated absorbed
RuntimeErrors as unhandled and reported them as faults.
"""

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
    """Defensive pin: every transport-class type plus CancelledError
    is still advertised alongside the new RuntimeError addition."""
    handled = AsyncAdaptedConnection._terminate_handled_exceptions()
    for cls in _TRANSPORT_CLASS_EXCEPTIONS:
        assert cls in handled, f"{cls.__name__} missing from terminate-handled tuple"
    assert asyncio.CancelledError in handled
