"""``cursor()`` raises InterfaceError (a dbapi.Error) on a closed adapter
instead of deferring to the first execute(), where a GC'd inner would
surface ReferenceError and bypass SA's _handle_dbapi_exception. close()
swaps ``self._connection`` to a ``weakref.proxy``, the signal it ran."""

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
    adapter._connection = weakref.proxy(inner)  # simulate close()'s swap
    with pytest.raises(InterfaceError, match="Connection is closed"):
        adapter.cursor()


def test_cursor_after_close_with_dead_inner_does_not_leak_referenceerror() -> None:
    """When the inner is GC'd, the front-line raise must still fire;
    otherwise the deferred execute() would raise ReferenceError."""

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
    """The InterfaceError message must keep the "connection is closed"
    substring so the dialect's is_disconnect matcher still trips it."""
    from sqlalchemydqlite.base import DqliteDialect

    err = InterfaceError("Connection is closed (id=42)")
    assert DqliteDialect().is_disconnect(err, None, None) is True
