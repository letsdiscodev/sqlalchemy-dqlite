"""Pin: closed-state proxy guard runs BEFORE the ``server_side=True``
reject in ``AsyncAdaptedConnection.cursor``.

A caller that invokes ``cursor(server_side=True)`` on an already-closed
adapter must see ``InterfaceError("Connection is closed ...")`` — the
actionable signal — rather than ``NotSupportedError("Server-side cursors
are not supported")``, which is a feature-availability diagnostic on a
connection that is no longer usable at all.

Mirrors the closed-state-precedence discipline applied elsewhere in the
codebase (e.g. dbapi-layer ``AsyncConnection.cursor`` ordering).
"""

from __future__ import annotations

import weakref
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import InterfaceError, NotSupportedError
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


def _close_adapter(adapter: AsyncAdaptedConnection) -> None:
    """Simulate the post-close shape: ``self._connection`` is a
    ``weakref.proxy`` (matches what ``close()`` does in production)."""
    target = adapter._connection
    adapter._connection = weakref.proxy(target)


def test_closed_adapter_server_side_true_raises_interface_error_not_notsupported() -> None:
    adapter = _make_adapter()
    _close_adapter(adapter)
    with pytest.raises(InterfaceError, match="Connection is closed"):
        adapter.cursor(server_side=True)


def test_closed_adapter_server_side_false_raises_interface_error() -> None:
    adapter = _make_adapter()
    _close_adapter(adapter)
    with pytest.raises(InterfaceError, match="Connection is closed"):
        adapter.cursor(server_side=False)


def test_live_adapter_server_side_true_raises_not_supported_error() -> None:
    """Sanity: with a live connection, ``server_side=True`` still
    raises ``NotSupportedError`` (the live-path feature reject)."""
    adapter = _make_adapter()
    with pytest.raises(NotSupportedError, match="Server-side"):
        adapter.cursor(server_side=True)


def test_live_adapter_server_side_false_returns_fresh_cursor() -> None:
    adapter = _make_adapter()
    cur = adapter.cursor(server_side=False)
    assert isinstance(cur, AsyncAdaptedCursor)
