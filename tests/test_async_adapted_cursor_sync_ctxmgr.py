"""Pin: ``AsyncAdaptedCursor`` sync context-manager protocol — ``__enter__``
returns the adapter, ``__exit__`` closes on both clean and error exit and
does not suppress caller exceptions.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_adapted_cursor_enter_returns_self() -> None:
    """``__enter__`` returns the adapter, not the underlying dbapi cursor."""
    cur = AsyncAdaptedCursor(MagicMock())
    with cur as bound:
        assert bound is cur


def test_adapted_cursor_exit_closes_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    """``__exit__`` calls ``self.close()`` on clean exit."""
    cur = AsyncAdaptedCursor(MagicMock())
    calls: list[AsyncAdaptedCursor] = []
    # __slots__ rejects per-instance method assignment; patch the class.
    monkeypatch.setattr(AsyncAdaptedCursor, "close", lambda self: calls.append(self))
    with cur:
        pass
    assert calls == [cur]


def test_adapted_cursor_exit_does_not_suppress_exception() -> None:
    """``__exit__`` must not swallow the caller's exception."""
    cur = AsyncAdaptedCursor(MagicMock())
    with pytest.raises(RuntimeError, match="body"), cur:
        raise RuntimeError("body")


def test_adapted_cursor_exit_closes_on_exception_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close`` runs even if the body raised, so the cursor can't leak."""
    cur = AsyncAdaptedCursor(MagicMock())
    calls: list[AsyncAdaptedCursor] = []
    monkeypatch.setattr(AsyncAdaptedCursor, "close", lambda self: calls.append(self))
    with pytest.raises(RuntimeError, match="body"), cur:
        raise RuntimeError("body")
    assert calls == [cur]
