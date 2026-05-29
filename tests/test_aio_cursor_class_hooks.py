"""Pin: ``AsyncAdaptedConnection`` exposes ``_cursor_cls`` / ``_ss_cursor_cls`` class hooks so
subclasses can swap the cursor class without re-implementing ``cursor()``."""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def test_async_adapted_connection_has_cursor_cls_hook() -> None:
    """``_cursor_cls`` is exposed at class scope."""
    assert AsyncAdaptedConnection._cursor_cls is AsyncAdaptedCursor


def test_async_adapted_connection_has_ss_cursor_cls_hook() -> None:
    """``_ss_cursor_cls`` exists for SA-introspection parity only; server-side is rejected
    before instantiation."""
    assert AsyncAdaptedConnection._ss_cursor_cls is AsyncAdaptedCursor


def test_cursor_method_uses_cursor_cls_hook() -> None:
    """``cursor()`` reads ``self._cursor_cls``, so a subclass override is observed."""

    class _MyCursor(AsyncAdaptedCursor):
        pass

    class _MyAdapter(AsyncAdaptedConnection):
        _cursor_cls = _MyCursor

    adapter = _MyAdapter.__new__(_MyAdapter)
    adapter._connection = MagicMock()

    cur = adapter.cursor()
    assert type(cur) is _MyCursor, (
        f"cursor() must instantiate via _cursor_cls hook; got {type(cur).__name__}"
    )


def test_cursor_hook_subclass_still_rejects_server_side() -> None:
    """The ``server_side=True`` reject sits above the hook, so a subclass swap can't bypass it."""
    import pytest

    from dqlitedbapi.exceptions import NotSupportedError

    class _MyCursor(AsyncAdaptedCursor):
        pass

    class _MyAdapter(AsyncAdaptedConnection):
        _cursor_cls = _MyCursor

    adapter = _MyAdapter.__new__(_MyAdapter)
    adapter._connection = MagicMock()

    with pytest.raises(NotSupportedError, match="Server-side"):
        adapter.cursor(server_side=True)


def test_cursor_hook_subclass_still_rejects_post_close() -> None:
    """The closed-state guard fires before ``self._cursor_cls(self)`` even with a subclass swap."""
    import weakref

    import pytest

    from dqlitedbapi.exceptions import InterfaceError

    class _MyCursor(AsyncAdaptedCursor):
        pass

    class _MyAdapter(AsyncAdaptedConnection):
        _cursor_cls = _MyCursor

    adapter = _MyAdapter.__new__(_MyAdapter)
    target = type("Inner", (), {})()
    adapter._connection = weakref.proxy(target)

    with pytest.raises(InterfaceError, match="Connection is closed"):
        adapter.cursor()
