"""Pin: ``_async_ping``'s close arm catches the same transport-class tuple
as the sync sibling — programmer bugs (``AttributeError`` etc.) propagate
rather than being silently DEBUG-logged."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio


async def test_async_ping_close_arm_propagates_attribute_error_from_refactor() -> None:
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.close = MagicMock(side_effect=AttributeError("simulated refactor break"))

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    with pytest.raises(AttributeError, match="simulated refactor break"):
        await greenlet_spawn(dialect.do_ping, dbapi_connection)


async def test_async_ping_close_arm_still_absorbs_oserror_transport_class() -> None:
    """Sanity: legitimate transport-class failures stay absorbed (ping returns True)."""
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.close = MagicMock(side_effect=OSError("simulated transport"))

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)  # OSError absorbed
    assert result is True
