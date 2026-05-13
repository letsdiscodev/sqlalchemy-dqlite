"""Pin: ``AsyncAdaptedConnection._handle_exception`` substring scan
is case-insensitive, mirroring the sibling ``is_disconnect`` site.

`base.py`'s ``is_disconnect`` lower-cases the message before
substring-scanning ``_dqlite_disconnect_messages`` (stored in
lower-case for that reason). The async remap site previously
substring-matched the raw ``str(error)`` without lower-casing, so a
Python minor that flipped the casing of the canonical wordings (e.g.
``Event Loop Is Closed`` instead of ``Event loop is closed``) would
silently bypass the remap and the cross-loop fault would leak as a
bare ``RuntimeError`` past SA's classifier.

This test pins the case-insensitive contract so a regression that
re-introduced raw-case matching trips a fast-feedback failure.
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
    # The original casing is preserved in the remapped wording's tail —
    # only the substring scan is case-insensitive.
    assert str(error) in str(excinfo.value)
