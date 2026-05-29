"""Pin the public-API happy paths on AsyncAdaptedConnection (the post-close guards
are covered elsewhere; this covers the success branches behind them)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_adapter() -> AsyncAdaptedConnection:
    """Adapter over a fake dbapi connection, skipping ``__init__``."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


def test_driver_connection_returns_underlying_when_open() -> None:
    adapter = _make_adapter()
    assert adapter.driver_connection is adapter._connection


def test_run_async_delegates_to_super_when_open() -> None:
    """``run_async`` delegates to ``super().run_async``; the expected MissingGreenlet
    (raised outside a greenlet) proves the guard passed and we did not return early."""
    from sqlalchemy.exc import MissingGreenlet

    adapter = _make_adapter()
    inner = adapter._connection

    def callback(dbapi_conn: object) -> object:
        assert dbapi_conn is inner
        return None

    with pytest.raises(MissingGreenlet):
        adapter.run_async(callback)


def test_cursor_server_side_true_raises_not_supported_error() -> None:
    """``server_side=True`` rejects as ``NotSupportedError`` (a PEP 249 ``dbapi.Error``)
    so cross-driver ``except dbapi.Error:`` catches it."""
    from dqlitedbapi.exceptions import NotSupportedError

    adapter = _make_adapter()
    with pytest.raises(NotSupportedError, match="Server-side"):
        adapter.cursor(server_side=True)


def test_execute_with_parameters_dispatches_to_cursor_execute() -> None:
    """The params-supplied branch of ``execute``; pinned via a ``cursor()``-overriding
    subclass since the slotted parent forbids per-instance method replacement."""

    class StubCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, operation: str, *args: object) -> None:
            self.calls.append((operation, args))

        def close(self) -> None:
            pass

    stub = StubCursor()

    class _StubbedAdapter(AsyncAdaptedConnection):
        def cursor(self, server_side: bool = False) -> object:  # type: ignore[override]
            return stub

    adapter = _StubbedAdapter.__new__(_StubbedAdapter)
    adapter._connection = MagicMock()

    result = adapter.execute("SELECT ?", [42])

    assert result is stub
    assert stub.calls == [("SELECT ?", ([42],))]


def test_async_adapted_cursor_construction_does_not_raise() -> None:
    adapter = _make_adapter()
    cur = adapter.cursor()
    assert isinstance(cur, AsyncAdaptedCursor)


def test_driver_connection_after_close_raises_interface_error() -> None:
    import weakref

    from dqlitedbapi import InterfaceError

    adapter = _make_adapter()
    inner = adapter._connection
    adapter._connection = weakref.proxy(inner)
    with pytest.raises(InterfaceError, match="closed"):
        _ = adapter.driver_connection


def test_run_async_after_close_raises_interface_error() -> None:
    import weakref

    from dqlitedbapi import InterfaceError

    adapter = _make_adapter()
    inner = adapter._connection
    adapter._connection = weakref.proxy(inner)
    with pytest.raises(InterfaceError, match="closed"):
        adapter.run_async(lambda dbapi: None)
