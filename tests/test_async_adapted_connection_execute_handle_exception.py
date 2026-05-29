"""Pin: ``AsyncAdaptedConnection.execute`` routes ``self.cursor()``
failures through ``_handle_exception``, like the cursor-level path.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapted() -> Any:
    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = MagicMock()
    return adapted


def test_execute_cursor_failure_routed_through_handle_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A loop-closed ``RuntimeError`` from ``cursor()`` is remapped to
    ``OperationalError`` so SA's ``is_disconnect`` invalidates the slot."""
    adapted = _make_adapted()

    def _boom(self: Any) -> Any:
        raise RuntimeError("Event loop is closed")

    monkeypatch.setattr(AsyncAdaptedConnection, "cursor", _boom)

    with pytest.raises(OperationalError) as excinfo:
        adapted.execute("SELECT 1")
    assert "event loop closed:" in str(excinfo.value)


def test_execute_cursor_runtime_error_cross_loop_remapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cross-loop ``RuntimeError`` from ``cursor()`` is remapped to
    ``OperationalError`` with the ``event-loop mismatch:`` prefix."""
    adapted = _make_adapted()

    def _boom(self: Any) -> Any:
        raise RuntimeError("Future <Future ...> attached to a different loop")

    monkeypatch.setattr(AsyncAdaptedConnection, "cursor", _boom)

    with pytest.raises(OperationalError) as excinfo:
        adapted.execute("SELECT 1")
    assert "event-loop mismatch:" in str(excinfo.value)


def test_execute_cursor_unrelated_error_not_remapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrelated error propagates as-is; only cross-loop faults remap."""
    adapted = _make_adapted()

    class _OurError(Exception):
        pass

    def _boom(self: Any) -> Any:
        raise _OurError("nothing to do with loops")

    monkeypatch.setattr(AsyncAdaptedConnection, "cursor", _boom)

    with pytest.raises(_OurError):
        adapted.execute("SELECT 1")
