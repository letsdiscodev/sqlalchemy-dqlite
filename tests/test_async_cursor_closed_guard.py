"""AsyncAdaptedCursor honours the PEP 249 closed-cursor contract on
setinputsizes / setoutputsize.

The underlying ``AsyncCursor`` raises ``InterfaceError`` when those
methods are called after ``close()``. The adapter previously
implemented both as bare ``pass`` statements, silently succeeding on
closed cursors and hiding the bug from callers migrating between the
two cursor types. It also kept the narrow "cursor is closed"
``InterfaceError`` branch in ``is_disconnect`` from firing on
adapter-managed cursors. A ``_closed`` flag — set in ``close()`` —
gates both methods.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _cursor() -> AsyncAdaptedCursor:
    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = MagicMock()
    return AsyncAdaptedCursor(adapted)


class TestSetInputSizesClosed:
    def test_open_cursor_still_no_ops(self) -> None:
        cursor = _cursor()
        cursor.setinputsizes([10, None, 20])  # must not raise

    def test_closed_cursor_raises(self) -> None:
        cursor = _cursor()
        cursor.close()
        with pytest.raises(InterfaceError, match="cursor is closed"):
            cursor.setinputsizes([10])


class TestSetOutputSizeClosed:
    def test_open_cursor_still_no_ops(self) -> None:
        cursor = _cursor()
        cursor.setoutputsize(1024)  # must not raise

    def test_closed_cursor_raises(self) -> None:
        cursor = _cursor()
        cursor.close()
        with pytest.raises(InterfaceError, match="cursor is closed"):
            cursor.setoutputsize(1024)


class TestClosedFlag:
    def test_initial_state(self) -> None:
        assert _cursor()._closed is False

    def test_close_sets_flag(self) -> None:
        cursor = _cursor()
        cursor.close()
        assert cursor._closed is True

    def test_double_close_is_idempotent(self) -> None:
        cursor = _cursor()
        cursor.close()
        cursor.close()  # must not raise
        assert cursor._closed is True
