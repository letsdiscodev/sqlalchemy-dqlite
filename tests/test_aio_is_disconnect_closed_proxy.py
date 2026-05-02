"""``DqliteDialect_aio.is_disconnect`` short-circuits on closed proxy.

After ``AsyncAdaptedConnection.close``, the inner ``_connection`` slot
is replaced by a ``weakref.proxy``. SA calling ``is_disconnect`` on
such an adapter should report ``True`` immediately rather than walking
the cause chain — the connection is already torn down.

Mirrors asyncpg's ``connection._connection.is_closed()`` short-circuit.
"""

import weakref

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _Dead:
    pass


def test_async_is_disconnect_fast_paths_closed_proxy() -> None:
    target = _Dead()
    stub = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    stub._connection = weakref.proxy(target)

    dialect = DqliteDialect_aio()
    err = Exception("benign; cause-walk would not classify as disconnect")

    assert dialect.is_disconnect(err, stub, None) is True


def test_async_is_disconnect_fast_paths_gc_dead_proxy() -> None:
    """Proxy whose target was GC'd — the deadest possible state.
    Fast-path must report True without dereferencing the proxy
    (which would raise ``ReferenceError``).
    """
    target = _Dead()
    stub = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    stub._connection = weakref.proxy(target)
    del target  # GC the underlying

    dialect = DqliteDialect_aio()
    err = Exception("anything")
    assert dialect.is_disconnect(err, stub, None) is True


def test_async_is_disconnect_falls_through_for_open_inner_connection() -> None:
    """When the inner connection is NOT a proxy, the override delegates
    to ``super().is_disconnect`` — the rich classifier in
    ``DqliteDialect.is_disconnect`` runs unchanged. A benign exception
    with no transport markers should classify False.
    """

    class FakeOpen:
        pass

    stub = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    stub._connection = FakeOpen()

    dialect = DqliteDialect_aio()
    err = Exception("not a transport error")
    assert dialect.is_disconnect(err, stub, None) is False


def test_async_is_disconnect_falls_through_for_no_connection() -> None:
    """When SA calls ``is_disconnect`` with ``connection=None``
    (e.g., during pre-ping when no checkout exists), the fast-path is
    skipped and the inherited classifier runs."""
    dialect = DqliteDialect_aio()
    err = Exception("not a transport error")
    assert dialect.is_disconnect(err, None, None) is False
