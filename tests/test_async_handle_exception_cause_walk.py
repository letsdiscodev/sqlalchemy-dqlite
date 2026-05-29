"""Pin: ``AsyncAdaptedConnection._handle_exception`` walks the
``__cause__`` / ``__context__`` chain when matching cross-loop wording, so
a wrapping layer can't hide the original cross-loop fault from the remap."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError, ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapted() -> Any:
    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = MagicMock()
    return adapted


def test_handle_exception_remaps_wrapped_event_loop_closed_via_cause() -> None:
    """A cross-loop ``RuntimeError`` chained as ``__cause__`` still remaps."""
    inner = RuntimeError("Event loop is closed")
    try:
        raise ProgrammingError("opaque wrapper") from inner
    except ProgrammingError as wrapped:
        adapted = _make_adapted()
        with pytest.raises(OperationalError) as excinfo:
            adapted._handle_exception(wrapped)
    assert "event loop closed:" in str(excinfo.value)


def test_handle_exception_remaps_wrapped_cross_loop_via_context() -> None:
    """A context-chained cross-loop ``RuntimeError`` (no ``from``) still remaps."""
    try:
        try:
            raise RuntimeError("Future <Future ...> attached to a different loop")
        except RuntimeError:
            # No ``from``: chaining lives on __context__, which the walk must traverse.
            raise ProgrammingError("opaque wrapper")  # noqa: B904
    except ProgrammingError as wrapped:
        adapted = _make_adapted()
        with pytest.raises(OperationalError) as excinfo:
            adapted._handle_exception(wrapped)
    assert "event-loop mismatch:" in str(excinfo.value)


def test_handle_exception_remaps_deeply_chained_cause() -> None:
    """Two-hop cause chain: the walk must still see the cross-loop cause."""
    bottom = RuntimeError("This event loop is already running")
    try:
        try:
            raise bottom
        except RuntimeError as rte:
            raise ProgrammingError("mid wrapper") from rte
    except ProgrammingError as mid:
        try:
            raise ProgrammingError("top wrapper") from mid
        except ProgrammingError as top:
            adapted = _make_adapted()
            with pytest.raises(OperationalError) as excinfo:
                adapted._handle_exception(top)
    assert "event loop already running:" in str(excinfo.value)


def test_handle_exception_unrelated_chain_does_not_misfire() -> None:
    """A chain with no cross-loop wording re-raises unchanged: the walk widens
    the match, it doesn't turn unrelated errors into ``OperationalError``."""
    inner = RuntimeError("unrelated")
    try:
        raise ProgrammingError("also unrelated") from inner
    except ProgrammingError as wrapped:
        adapted = _make_adapted()
        with pytest.raises(ProgrammingError):
            adapted._handle_exception(wrapped)
