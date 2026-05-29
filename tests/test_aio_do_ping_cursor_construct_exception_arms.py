"""Pin: async ``do_ping`` mirrors the sync exception arms when inner ``cursor()`` raises,
including ``DatabaseError`` gated on ``_BARE_DBE_DISCONNECT_CODES``."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio
from sqlalchemydqlite.base import _BARE_DBE_DISCONNECT_CODES


def _make_adapter_with_cursor_raise(exc: BaseException) -> Any:
    inner = MagicMock()
    inner.cursor.side_effect = exc
    dbapi_connection: Any = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    dbapi_connection._connection = inner
    return dbapi_connection


@pytest.mark.parametrize(
    "exc",
    [
        OSError("ECONNRESET"),
        InterfaceError("Connection is closed (id=...)"),
        OperationalError("transient"),
        ProgrammingError("cross-loop reuse"),
        DqliteConnectionError("transport down"),
    ],
)
async def test_do_ping_returns_false_on_transport_class_cursor_raise(
    exc: BaseException,
) -> None:
    adapter = _make_adapter_with_cursor_raise(exc)
    dialect = DqliteDialect_aio()
    result = await greenlet_spawn(dialect.do_ping, adapter)
    assert result is False


@pytest.mark.parametrize("code", list(_BARE_DBE_DISCONNECT_CODES))
async def test_do_ping_returns_false_on_databaseerror_disconnect_code(
    code: int,
) -> None:
    """``DatabaseError`` with a disconnect code classifies as ping-fail."""
    adapter = _make_adapter_with_cursor_raise(DatabaseError("CORRUPT: image truncated", code=code))
    dialect = DqliteDialect_aio()
    result = await greenlet_spawn(dialect.do_ping, adapter)
    assert result is False


async def test_do_ping_propagates_databaseerror_non_disconnect_code() -> None:
    """``DatabaseError`` with a non-disconnect code is a real fault and must propagate."""
    adapter = _make_adapter_with_cursor_raise(DatabaseError("not-found shape", code=12))
    dialect = DqliteDialect_aio()
    with pytest.raises(DatabaseError):
        await greenlet_spawn(dialect.do_ping, adapter)
