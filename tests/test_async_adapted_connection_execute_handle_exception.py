"""Pin: ``AsyncAdaptedConnection.execute`` routes ``self.cursor()``
failures through ``_handle_exception``.

The cursor-level path (``AsyncAdaptedCursor.execute``) wraps the
entire try-frame with ``self._adapt_connection._handle_exception``
so cross-loop / loop-closed RuntimeErrors get remapped to
``OperationalError`` before SA's classifier sees them.
``AsyncAdaptedConnection.execute`` is the convenience method that
SA-internal code paths (e.g. ``dialects/sqlite/provision.py``)
invoke directly; it previously DID NOT route ``cursor()`` failures
through the same hook, so the same fault remapped differently
depending on which entry point was taken. Pin the single-discipline
restructure.
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
    """A ``RuntimeError("Event loop is closed")`` raised by
    ``self.cursor()`` must be remapped to ``OperationalError`` via
    ``_handle_exception`` — mirroring the cursor-level execute path —
    so SA's ``is_disconnect`` classifier sees a dbapi.Error subclass
    and the pool invalidates the slot.
    """
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
    ``OperationalError`` with the ``event-loop mismatch:`` prefix.
    """
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
    """An unrelated error (no cross-loop wording) propagates as-is —
    the routing widens the cross-loop remap, it doesn't blanket-rewrap
    every failure as ``OperationalError``.
    """
    adapted = _make_adapted()

    class _OurError(Exception):
        pass

    def _boom(self: Any) -> Any:
        raise _OurError("nothing to do with loops")

    monkeypatch.setattr(AsyncAdaptedConnection, "cursor", _boom)

    with pytest.raises(_OurError):
        adapted.execute("SELECT 1")
