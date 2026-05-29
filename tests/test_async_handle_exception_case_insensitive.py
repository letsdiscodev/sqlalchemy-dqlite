"""Pin: ``AsyncAdaptedConnection._handle_exception`` substring scan is
case-insensitive (like the sibling ``is_disconnect``), so a Python minor
flipping the canonical wording's casing can't bypass the remap."""

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


@pytest.mark.parametrize(
    ("error", "expected_substring"),
    [
        (
            RuntimeError("Future ATTACHED TO A DIFFERENT LOOP"),
            "event-loop mismatch:",
        ),
        (
            RuntimeError("EVENT LOOP IS CLOSED"),
            "event loop closed:",
        ),
        (
            RuntimeError("This Event Loop Is Already Running"),
            "event loop already running:",
        ),
        (
            ProgrammingError("connection ATTACHED to a Different Event Loop"),
            "event-loop mismatch:",
        ),
    ],
)
def test_handle_exception_remaps_regardless_of_message_casing(
    error: Exception, expected_substring: str
) -> None:
    adapted = _make_adapted()
    with pytest.raises(OperationalError) as excinfo:
        adapted._handle_exception(error)
    assert expected_substring in str(excinfo.value), (
        f"expected remap substring {expected_substring!r} in {excinfo.value!s}"
    )
    # Only the scan is case-insensitive; the original casing survives in the tail.
    assert str(error) in str(excinfo.value)
