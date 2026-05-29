"""``DqliteDialect_aio.is_disconnect`` reports True immediately when the
inner ``_connection`` is a ``weakref.proxy`` (set by close), instead of
walking the cause chain. Mirrors asyncpg's is_closed() short-circuit."""

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
    """A proxy whose target was GC'd: fast-path must report True without
    dereferencing it (which would raise ReferenceError)."""
    target = _Dead()
    stub = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    stub._connection = weakref.proxy(target)
    del target  # GC the underlying

    dialect = DqliteDialect_aio()
    err = Exception("anything")
    assert dialect.is_disconnect(err, stub, None) is True


def test_async_is_disconnect_falls_through_for_open_inner_connection() -> None:
    """When the inner connection is not a proxy, the override delegates to
    ``super().is_disconnect``; a benign exception classifies False."""

    class FakeOpen:
        pass

    stub = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    stub._connection = FakeOpen()

    dialect = DqliteDialect_aio()
    err = Exception("not a transport error")
    assert dialect.is_disconnect(err, stub, None) is False


def test_async_is_disconnect_falls_through_for_no_connection() -> None:
    """With ``connection=None`` (e.g. pre-ping with no checkout) the
    fast-path is skipped and the inherited classifier runs."""
    dialect = DqliteDialect_aio()
    err = Exception("not a transport error")
    assert dialect.is_disconnect(err, None, None) is False
