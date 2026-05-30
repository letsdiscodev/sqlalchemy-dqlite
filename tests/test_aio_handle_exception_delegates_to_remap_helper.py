"""``_handle_exception`` remaps the four loop-state RuntimeError/ProgrammingError
substring patterns to OperationalError and propagates everything else unchanged."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError, ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    from unittest.mock import MagicMock

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


@pytest.mark.parametrize(
    ("phrase", "expected_prefix"),
    [
        ("got Future <Future> attached to a different loop", "event-loop mismatch"),
        ("different event loop", "event-loop mismatch"),
        ("Event loop is closed", "event loop closed"),
        ("This event loop is already running", "event loop already running"),
    ],
)
def test_handle_exception_remaps_runtime_error_via_helper(
    phrase: str, expected_prefix: str
) -> None:
    """``_handle_exception`` raises the expected OperationalError for each
    of the four substring patterns."""
    adapter = _make_adapter()
    original = RuntimeError(phrase)
    with pytest.raises(OperationalError, match=expected_prefix):
        adapter._handle_exception(original)


def test_handle_exception_propagates_non_loop_runtime_error_unchanged() -> None:
    """A RuntimeError with none of the four substrings propagates
    unchanged."""
    adapter = _make_adapter()
    original = RuntimeError("some unrelated runtime fault")
    with pytest.raises(RuntimeError, match="some unrelated runtime fault"):
        adapter._handle_exception(original)


def test_handle_exception_propagates_non_runtime_error_unchanged() -> None:
    """A non-RuntimeError / non-ProgrammingError class is skipped by the
    helper's isinstance gate; original propagates."""
    adapter = _make_adapter()
    original = ValueError("bare ValueError")
    with pytest.raises(ValueError, match="bare ValueError"):
        adapter._handle_exception(original)


def test_handle_exception_remaps_programmingerror_with_loop_phrase() -> None:
    """The helper's isinstance gate also catches ProgrammingError
    (cross-loop wrap from dqlitedbapi)."""
    adapter = _make_adapter()
    original = ProgrammingError("AsyncConnection in use by a different event loop")
    with pytest.raises(OperationalError, match="event-loop mismatch"):
        adapter._handle_exception(original)
