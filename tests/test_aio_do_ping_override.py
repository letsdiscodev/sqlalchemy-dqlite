"""``DqliteDialect_aio`` overrides ``do_ping`` to run ``SELECT 1`` in a single async hop and
route loop-state RuntimeErrors through ``_handle_exception`` so SA evicts the broken slot."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio


def test_aio_dialect_overrides_do_ping_locally() -> None:
    """The override must live on the async dialect's own class, not be inherited via MRO."""
    assert "do_ping" in DqliteDialect_aio.__dict__


async def test_do_ping_select_1_returns_true_on_healthy_connection() -> None:
    """Healthy path: execute alone proves the RTT (no fetchone), and do_ping returns True."""
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(1,))
    cursor.close = MagicMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is True
    cursor.execute.assert_awaited_once_with("SELECT 1")
    cursor.fetchone.assert_not_awaited()
    cursor.close.assert_called_once()


async def test_do_ping_returns_false_on_loop_state_runtime_error() -> None:
    """Loop-state RuntimeError classifies as ping-fail (False) via the OperationalError remap."""
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=RuntimeError("got Future attached to a different loop"))
    cursor.close = MagicMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    from sqlalchemydqlite.aio import AsyncAdaptedConnection

    dbapi_connection: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False


async def test_do_ping_returns_false_on_operational_error() -> None:
    """``OperationalError`` classifies as ping-fail."""
    from dqlitedbapi.exceptions import OperationalError as DbapiOperationalError

    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=DbapiOperationalError("connection lost"))
    cursor.close = MagicMock()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False


async def test_do_ping_returns_false_on_oserror_from_cursor_call() -> None:
    """An OSError from the inner ``cursor()`` call classifies as ping-fail, not a leak."""
    dialect = DqliteDialect_aio()

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(side_effect=OSError("ECONNRESET"))

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is False
