"""Pin: ``AsyncAdaptedConnection`` exposes class-level
``_cursor_cls`` / ``_ss_cursor_cls`` extension hooks matching SA's
reference connector at ``sqlalchemy/connectors/asyncio.py:336-337``.

Dialect subclasses (asyncpg pattern, sqlalchemy-utils' instrumented
adapters, third-party SA-async instrumentation) swap the cursor
class by overriding these attributes at class scope without
re-implementing ``cursor()``. Without the hooks, every subclass
must override ``cursor()`` and re-implement the server_side reject
+ closed-state guard inline.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def test_async_adapted_connection_has_cursor_cls_hook() -> None:
    """The class exposes ``_cursor_cls`` at class scope so subclasses
    can override it without re-implementing ``cursor()``."""
    assert AsyncAdaptedConnection._cursor_cls is AsyncAdaptedCursor


def test_async_adapted_connection_has_ss_cursor_cls_hook() -> None:
    """The class exposes ``_ss_cursor_cls`` for SA-introspection parity.
    The dialect pins ``supports_server_side_cursors=False`` and
    ``cursor(server_side=True)`` short-circuits on
    ``NotSupportedError`` before instantiation, so this attribute is
    for the SA reflection surface only (it must exist for
    SA-introspection code that reads it)."""
    assert AsyncAdaptedConnection._ss_cursor_cls is AsyncAdaptedCursor


def test_cursor_method_uses_cursor_cls_hook() -> None:
    """``cursor()`` reads ``self._cursor_cls`` rather than hard-coding
    ``AsyncAdaptedCursor``. A subclass that overrides
    ``_cursor_cls`` must observe the swap."""

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
