"""``_terminate_handled_exceptions()`` exposes the catch tuple the hand-rolled
``terminate()`` body uses, for third-party SA-async introspection tooling."""

from __future__ import annotations

import asyncio

from sqlalchemydqlite.aio import AsyncAdaptedConnection
from sqlalchemydqlite.base import _TRANSPORT_CLASS_EXCEPTIONS


def test_terminate_handled_exceptions_method_exists() -> None:
    assert hasattr(AsyncAdaptedConnection, "_terminate_handled_exceptions")


def test_terminate_handled_exceptions_returns_expected_tuple() -> None:
    handled = AsyncAdaptedConnection._terminate_handled_exceptions()
    assert isinstance(handled, tuple)
    for cls in _TRANSPORT_CLASS_EXCEPTIONS:
        assert cls in handled
    assert asyncio.CancelledError in handled
