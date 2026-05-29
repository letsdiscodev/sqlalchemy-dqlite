"""is_disconnect classifies an OperationalError with the ``"event loop closed"`` substring."""

from __future__ import annotations

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def test_event_loop_closed_substring_classifies_as_disconnect() -> None:
    e = OperationalError(
        "event loop closed: Event loop is closed",
        code=None,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_event_loop_closed_case_insensitive() -> None:
    e = OperationalError("Event Loop Closed: Event loop is closed", code=None)
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_unrelated_event_loop_message_not_classified() -> None:
    """A message mentioning "event loop" but not "event loop closed" must not match."""
    e = OperationalError("event loop is happy and healthy", code=None)
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_event_loop_closed_via_cause_walk() -> None:
    """The substring scan walks __cause__/__context__, so a wrapped error still classifies."""
    inner = OperationalError("event loop closed: Event loop is closed", code=None)

    class _SAWrapperError(Exception):
        pass

    outer = _SAWrapperError("statement execution failed")
    outer.__cause__ = inner
    assert DqliteDialect().is_disconnect(outer, None, None) is True
