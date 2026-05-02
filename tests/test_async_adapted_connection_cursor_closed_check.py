"""Pin: ``AsyncAdaptedConnection.cursor()`` raises
``InterfaceError`` (inside the dbapi.Error hierarchy) on a closed
adapter, instead of returning a live wrapper that defers the
diagnostic to the first ``execute()`` — at which point a GC'd inner
connection would surface ``ReferenceError`` (NOT a dbapi.Error
subclass), bypassing SA's ``_handle_dbapi_exception`` classifier.

The detection is structural: ``close()`` replaces ``self._connection``
with a ``weakref.proxy`` to allow GC of the inner conn. The
``isinstance(..., weakref.ProxyTypes)`` check is the canonical signal
that close has run.
"""

from __future__ import annotations

import weakref
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_cursor_on_open_adapter_returns_cursor() -> None:
    """Positive control: an open adapter still returns a cursor."""
    inner = MagicMock()
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = inner
    cur = adapter.cursor()
    assert cur is not None


def test_cursor_after_close_raises_interface_error_immediately() -> None:
    """Pin the front-line raise."""
    inner = MagicMock()
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = inner
    # Simulate close()'s proxy substitution.
    adapter._connection = weakref.proxy(inner)
    with pytest.raises(InterfaceError, match="Connection is closed"):
        adapter.cursor()


def test_cursor_after_close_with_dead_inner_does_not_leak_referenceerror() -> None:
    """The original failure mode: inner has been GC'd and the proxy
    points at nothing. Without the front-line check, the deferred
    ``cursor.execute()`` call would raise ``ReferenceError`` outside
    the dbapi.Error hierarchy. Pin that the front-line raise fires
    even when the inner is dead."""

    class _Inner:
        pass

    inner = _Inner()
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = weakref.proxy(inner)
    del inner  # triggers GC of the proxy target
    import gc

    gc.collect()
    with pytest.raises(InterfaceError, match="Connection is closed"):
        adapter.cursor()


def test_classifier_classifies_the_new_diagnostic() -> None:
    """Defence pin: the new InterfaceError shape still trips the
    SA dialect's is_disconnect substring matcher (which relies on
    ``"connection is closed"`` substring). A regression that drops
    the substring would surface as a pool slot leak."""
    from sqlalchemydqlite.base import DqliteDialect

    err = InterfaceError("Connection is closed (id=42)")
    assert DqliteDialect().is_disconnect(err, None, None) is True
