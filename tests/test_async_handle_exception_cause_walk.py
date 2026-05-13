"""Pin: ``AsyncAdaptedConnection._handle_exception`` walks the
``__cause__`` / ``__context__`` chain when matching cross-loop /
loop-closed / nested-loop wording.

The sibling ``DqliteDialect.is_disconnect`` walks the cause chain
via ``_walk_cause_chain`` so a wrapping layer (retry decorator,
telemetry middleware, dbapi wrapper) doesn't hide the original
cross-loop fault. The async remap site previously matched only the
direct exception's ``str()``, so an explicit
``raise FooError(...) from RuntimeError("Event loop is closed")``
would silently bypass the remap, leaking a non-dbapi error past
SA's classifier and stranding the broken slot.
"""

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
    """``RuntimeError("Event loop is closed")`` chained as ``__cause__``
    of a wrapping ``ProgrammingError`` must still trigger the remap.
    """
    inner = RuntimeError("Event loop is closed")
    try:
        raise ProgrammingError("opaque wrapper") from inner
    except ProgrammingError as wrapped:
        adapted = _make_adapted()
        with pytest.raises(OperationalError) as excinfo:
            adapted._handle_exception(wrapped)
    assert "event loop closed:" in str(excinfo.value)


def test_handle_exception_remaps_wrapped_cross_loop_via_context() -> None:
    """An implicit context-chained cross-loop ``RuntimeError`` (i.e.
    raised inside an except handler that didn't use ``from``) must
    still trigger the remap.
    """
    try:
        try:
            raise RuntimeError("Future <Future ...> attached to a different loop")
        except RuntimeError:
            # Intentional: no ``from`` so the chaining lives on
            # ``__context__`` rather than ``__cause__`` — the walk
            # must traverse both.
            raise ProgrammingError("opaque wrapper")  # noqa: B904
    except ProgrammingError as wrapped:
        adapted = _make_adapted()
        with pytest.raises(OperationalError) as excinfo:
            adapted._handle_exception(wrapped)
    assert "event-loop mismatch:" in str(excinfo.value)


def test_handle_exception_remaps_deeply_chained_cause() -> None:
    """Two-hop cause chain: middle wrapper hides the cross-loop cause
    behind itself; the walk must still see it.
    """
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
    """An exception chain with no cross-loop wording must re-raise
    unchanged — the cause-walk widens the substring match, it does
    not turn unrelated errors into ``OperationalError``.
    """
    inner = RuntimeError("unrelated")
    try:
        raise ProgrammingError("also unrelated") from inner
    except ProgrammingError as wrapped:
        adapted = _make_adapted()
        with pytest.raises(ProgrammingError):
            adapted._handle_exception(wrapped)
