"""``DqliteDialect_aio`` overrides ``do_ping`` for the async fast path.

The inherited sync ``DqliteDialect.do_ping`` routes through the
``AsyncAdaptedCursor`` adapter and pays three ``await_only`` hops per
checkout. The override runs ``SELECT 1`` directly through the dbapi
async cursor in a single hop and routes loop-state RuntimeErrors
through the adapter's ``_handle_exception`` so SA's ``is_disconnect``
classifier evicts the broken slot.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio


def test_aio_dialect_overrides_do_ping_locally() -> None:
    """Drift defence: the override must live on the async dialect's
    own class, not be picked up via MRO. A future refactor that moves
    the override would be caught here.
    """
    assert "do_ping" in DqliteDialect_aio.__dict__


async def test_do_ping_select_1_returns_true_on_healthy_connection() -> None:
    """Healthy path: ``SELECT 1`` succeeds, ``do_ping`` returns True."""
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(1,))
    cursor.close = AsyncMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is True
    cursor.execute.assert_awaited_once_with("SELECT 1")
    cursor.fetchone.assert_awaited_once()
    cursor.close.assert_awaited_once()


async def test_do_ping_returns_false_on_loop_state_runtime_error() -> None:
    """Loop-state RuntimeError must classify as ping-fail (return
    False) rather than propagate. ``_handle_exception`` re-raises as
    ``OperationalError``; the outer ``do_ping`` catches that arm.
    """
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=RuntimeError("got Future attached to a different loop"))
    cursor.close = AsyncMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    # ``_handle_exception`` re-raises as OperationalError; the
    # adapter's plumbing is exercised directly via the
    # ``dbapi_connection`` mock — wire the override up to the real
    # AsyncAdaptedConnection method by binding it.
    from sqlalchemydqlite.aio import AsyncAdaptedConnection

    dbapi_connection: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False


async def test_do_ping_returns_false_on_operational_error() -> None:
    """``OperationalError`` (transport/server errors classified as
    disconnect by the dbapi) classifies as ping-fail."""
    from dqlitedbapi.exceptions import OperationalError as DbapiOperationalError

    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=DbapiOperationalError("connection lost"))
    cursor.close = AsyncMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False


async def test_do_ping_returns_false_on_oserror_from_cursor_call() -> None:
    """Drift defence: a live socket-RST during ``cursor()`` itself
    (raised from the inner sync ``cursor()`` call inside
    ``_async_ping``) must classify as ping-fail. The wrapper's
    ``OSError`` arm absorbs it without leaking past
    ``_do_ping_w_event``'s ``loaded_dbapi.Error`` filter."""
    dialect = DqliteDialect_aio()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(side_effect=OSError("ECONNRESET"))

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False
