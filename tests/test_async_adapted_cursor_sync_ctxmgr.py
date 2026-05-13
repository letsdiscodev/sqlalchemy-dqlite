"""Pin: ``AsyncAdaptedCursor`` sync context-manager protocol.

The adapter implements both sync (``__enter__`` / ``__exit__``) and
async ctxmgr arms; the async arm is pinned by
``tests/test_async_cursor_closed_guards.py`` and friends, but the sync
arm has no fast-feedback test. Four invariants matter and would all
look correct on cursory review yet break on refactor:

1. ``__enter__`` returns the adapter (not the underlying dbapi cursor).
2. ``__exit__`` calls ``self.close()`` on clean exit.
3. ``__exit__`` does NOT suppress caller exceptions.
4. ``__exit__`` runs ``close`` even when the body raised.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_adapted_cursor_enter_returns_self() -> None:
    """``__enter__`` must return the adapter so the body's
    ``cur.execute`` resolves to the adapter API surface, not the
    underlying dbapi cursor.
    """
    cur = AsyncAdaptedCursor(MagicMock())
    with cur as bound:
        assert bound is cur


def test_adapted_cursor_exit_closes_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    """``__exit__`` calls ``self.close()`` on clean exit."""
    cur = AsyncAdaptedCursor(MagicMock())
    calls: list[AsyncAdaptedCursor] = []
    # ``AsyncAdaptedCursor`` uses ``__slots__`` so per-instance method
    # assignment is rejected; patch at the class level for the test.
    monkeypatch.setattr(AsyncAdaptedCursor, "close", lambda self: calls.append(self))
    with cur:
        pass
    assert calls == [cur]


def test_adapted_cursor_exit_does_not_suppress_exception() -> None:
    """A truthy ``__exit__`` return would swallow the caller's
    exception. Pin: the body exception propagates.
    """
    cur = AsyncAdaptedCursor(MagicMock())
    with pytest.raises(RuntimeError, match="body"), cur:
        raise RuntimeError("body")


def test_adapted_cursor_exit_closes_on_exception_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close`` runs even if the body raised — otherwise the
    underlying dbapi cursor would leak through the ``with`` exit.
    """
    cur = AsyncAdaptedCursor(MagicMock())
    calls: list[AsyncAdaptedCursor] = []
    monkeypatch.setattr(AsyncAdaptedCursor, "close", lambda self: calls.append(self))
    with pytest.raises(RuntimeError, match="body"), cur:
        raise RuntimeError("body")
    assert calls == [cur]
