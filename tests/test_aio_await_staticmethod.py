"""``AsyncAdaptedConnection.await_`` is exposed as a staticmethod.

SA convention (asyncpg.py:714, aiosqlite.py:257,
connectors/asyncio.py:338): every reference async adapter exposes
``await_`` as a staticmethod on the connection class so external
instrumentation and the cursor can reuse the adapter's greenlet hop
without re-running detection. Pin the surface so a future cleanup that
removes the attribute as "unused" trips this regression guard.
"""

from sqlalchemy.util import await_only

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_async_adapter_exposes_await_class_attribute() -> None:
    """``await_`` lives on the class itself, not just instances —
    third-party instrumentation reads it via class introspection."""
    assert "await_" in AsyncAdaptedConnection.__dict__


def test_async_adapter_await_is_await_only() -> None:
    """The staticmethod points at SA's ``await_only`` (the canonical
    greenlet-aware shim) — not at our own wrapper."""
    assert AsyncAdaptedConnection.await_ is await_only


def test_async_adapter_await_is_callable_on_class() -> None:
    """Staticmethod descriptors return the underlying function on
    attribute access from the class — confirm the unwrap works so
    ``AsyncAdaptedConnection.await_(coro)`` is callable."""
    assert callable(AsyncAdaptedConnection.await_)
