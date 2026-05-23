"""Pin: async ``_async_ping``'s cursor-close arm catches the SAME
transport-class tuple as the sync sibling — programmer bugs from a
refactor regression (``AttributeError`` / ``TypeError`` /
``ValueError``) propagate rather than being silently DEBUG-logged.

The prior ``except (Exception, asyncio.CancelledError)`` swallowed
shapes the sync sibling at ``base.py`` (``do_ping``) deliberately
lets propagate.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio


async def test_async_ping_close_arm_propagates_attribute_error_from_refactor() -> None:
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    # Simulate a refactor break: ``cursor.close`` raises AttributeError.
    cursor.close = MagicMock(side_effect=AttributeError("simulated refactor break"))

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    with pytest.raises(AttributeError, match="simulated refactor break"):
        await greenlet_spawn(dialect.do_ping, dbapi_connection)


async def test_async_ping_close_arm_still_absorbs_oserror_transport_class() -> None:
    """Sanity: legitimate transport-class failures stay absorbed
    (DEBUG-logged, ping still returns True)."""
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.close = MagicMock(side_effect=OSError("simulated transport"))

    inner_conn = MagicMock()
    inner_conn.cursor = MagicMock(return_value=cursor)

    dbapi_connection = MagicMock()
    dbapi_connection._connection = inner_conn

    # No raise — the OSError is absorbed at the close arm.
    result = await greenlet_spawn(dialect.do_ping, dbapi_connection)
    assert result is True
