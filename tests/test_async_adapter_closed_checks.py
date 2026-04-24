"""``AsyncAdaptedCursor``'s fetch / optional-extension methods must
raise ``InterfaceError("cursor is closed")`` after the cursor is
closed. The adapter buffers rows into its own deque so a closed cursor
would previously return silently-drained-empty lists; PEP 249 §6.1.2
says operations on a closed cursor must raise.
"""

from __future__ import annotations

from collections import deque

import pytest

from dqlitedbapi import InterfaceError, NotSupportedError
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _closed_cursor() -> AsyncAdaptedCursor:
    cur = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
    cur._rows = deque()
    cur.arraysize = 1
    cur._closed = True
    return cur


def test_fetchone_after_close_raises() -> None:
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _closed_cursor().fetchone()


def test_fetchmany_after_close_raises() -> None:
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _closed_cursor().fetchmany()


def test_fetchall_after_close_raises() -> None:
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _closed_cursor().fetchall()


def test_callproc_after_close_raises_interface_not_not_supported() -> None:
    """On a closed cursor, the InterfaceError must be preferred over
    NotSupportedError — the closed-state is the more actionable root
    cause for the user.
    """
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _closed_cursor().callproc("p")


def test_nextset_after_close_raises_interface_not_not_supported() -> None:
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _closed_cursor().nextset()


def test_scroll_after_close_raises_interface_not_not_supported() -> None:
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _closed_cursor().scroll(1)


def test_callproc_on_open_cursor_still_raises_not_supported() -> None:
    """Regression guard: the open-cursor behaviour (raise
    NotSupportedError) must survive the closed-check addition."""
    cur = AsyncAdaptedCursor.__new__(AsyncAdaptedCursor)
    cur._rows = deque()
    cur.arraysize = 1
    cur._closed = False
    with pytest.raises(NotSupportedError):
        cur.callproc("p")
