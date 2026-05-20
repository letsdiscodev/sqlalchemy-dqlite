"""Pin the public-API happy paths on AsyncAdaptedConnection.

The closed-state guards on ``driver_connection``, ``run_async``, and
``cursor()`` were already covered by tests that drive the post-close
``weakref.proxy`` arm. Their *success* paths (return after the guard)
were not — the coverage report listed:

- ``driver_connection``: ``return self._connection`` (success branch)
- ``run_async``: ``return super().run_async(fn)`` (greenlet-bridge passthrough)
- ``cursor``: ``raise NotSupportedError(...)`` for ``server_side=True``
- ``execute``: ``cur.execute(operation, parameters)`` (params branch)

Without these pins, a refactor to either guard could silently break
the success path and leave only an integration-test signal. They are
not defensive code — they are the documented public-API surface SA
calls into.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_adapter() -> AsyncAdaptedConnection:
    """Construct an adapter with a fake underlying dbapi connection.
    Skips ``__init__`` (which expects a real awaitable connect path)
    and assigns the inner-connection slot directly. Mirrors the
    pattern used in ``test_aio_close_terminate_outside_greenlet.py``.
    """
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


def test_driver_connection_returns_underlying_when_open() -> None:
    """The success path of the property: when ``self._connection`` is
    not a ``weakref.proxy``, return it directly. The closed-state
    branch is covered separately by post-close tests."""
    adapter = _make_adapter()
    assert adapter.driver_connection is adapter._connection


def test_run_async_delegates_to_super_when_open() -> None:
    """``run_async(fn)`` after the closed-state guard delegates to
    ``super().run_async(fn)`` (SA's ``AdaptedConnection.run_async``).
    Without monkey-patching SA, drive the call and let SA invoke
    ``await_only(fn(self._connection))`` — outside a greenlet that
    raises ``MissingGreenlet``. We assert the guard passed and the
    delegation reached SA's path; the MissingGreenlet from there is
    expected and proves we did not return early."""
    from sqlalchemy.exc import MissingGreenlet

    adapter = _make_adapter()
    inner = adapter._connection

    def callback(dbapi_conn: object) -> object:
        # If we got here, super().run_async happened to invoke our fn.
        # On SA's side ``await_only`` will raise MissingGreenlet
        # because we're not inside a greenlet — the function-call
        # arrival is the success signal we care about.
        assert dbapi_conn is inner
        return None

    with pytest.raises(MissingGreenlet):
        adapter.run_async(callback)


def test_cursor_server_side_true_raises_not_supported_error() -> None:
    """The dialect pins ``supports_server_side_cursors=False``, so SA
    itself never passes ``server_side=True``. Third-party callers
    (and future SA paths) might; pin the explicit reject as
    ``NotSupportedError`` (a PEP 249 ``dbapi.Error`` subclass) so the
    rejection routes through SA's ``_handle_dbapi_exception``
    classifier and cross-driver ``except dbapi.Error:`` catches it.
    Matches the discipline of sibling cursor surface (``callproc`` /
    ``nextset`` / ``scroll`` all raise ``NotSupportedError``)."""
    from dqlitedbapi.exceptions import NotSupportedError

    adapter = _make_adapter()
    with pytest.raises(NotSupportedError, match="Server-side"):
        adapter.cursor(server_side=True)


def test_execute_with_parameters_dispatches_to_cursor_execute() -> None:
    """The params-supplied branch (``cur.execute(operation, parameters)``)
    sits behind a None-check on ``parameters``. Existing tests likely
    drive the no-params branch only. Pin the params branch via a
    subclass that overrides ``cursor()`` (the parent uses
    ``__slots__`` so per-instance method replacement is not allowed)."""

    class StubCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, operation: str, *args: object) -> None:
            self.calls.append((operation, args))

        def close(self) -> None:
            pass

    stub = StubCursor()

    class _StubbedAdapter(AsyncAdaptedConnection):
        # Mirrors the slots discipline of the parent; no extra slots.
        def cursor(self, server_side: bool = False) -> object:  # type: ignore[override]
            return stub

    adapter = _StubbedAdapter.__new__(_StubbedAdapter)
    adapter._connection = MagicMock()

    result = adapter.execute("SELECT ?", [42])

    assert result is stub
    assert stub.calls == [("SELECT ?", ([42],))]


def test_async_adapted_cursor_construction_does_not_raise() -> None:
    """The cursor() success path returns a fresh AsyncAdaptedCursor
    over the inner connection. Pin the success path; the closed-state
    branch is covered below."""
    adapter = _make_adapter()
    cur = adapter.cursor()
    assert isinstance(cur, AsyncAdaptedCursor)


def test_driver_connection_after_close_raises_interface_error() -> None:
    """The closed-state guard (``self._connection`` is a
    ``weakref.proxy``) on ``driver_connection`` raises
    ``InterfaceError``. Pin the raise arm symmetrically with the
    ``run_async`` and ``cursor`` closed-state pins."""
    import weakref

    from dqlitedbapi import InterfaceError

    adapter = _make_adapter()
    inner = adapter._connection
    adapter._connection = weakref.proxy(inner)
    with pytest.raises(InterfaceError, match="closed"):
        _ = adapter.driver_connection


def test_run_async_after_close_raises_interface_error() -> None:
    """The closed-state guard on ``run_async`` raises
    ``InterfaceError`` symmetrically with ``driver_connection``."""
    import weakref

    from dqlitedbapi import InterfaceError

    adapter = _make_adapter()
    inner = adapter._connection
    adapter._connection = weakref.proxy(inner)
    with pytest.raises(InterfaceError, match="closed"):
        adapter.run_async(lambda dbapi: None)
