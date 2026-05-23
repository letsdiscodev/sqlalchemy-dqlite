"""Pin: ``DqliteDialect_aio._async_ping`` calls cursor.execute exactly
once and does NOT call fetchone — matches sync sibling
``DqliteDialect.do_ping`` and SA's ``DefaultDialect.do_ping`` shape.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio


@pytest.mark.asyncio
async def test_async_ping_calls_execute_once_and_not_fetchone() -> None:
    dialect = DqliteDialect_aio()

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock()
    cursor.close = MagicMock()  # sync per AsyncCursor's documented shape

    raw = MagicMock()
    raw.cursor.return_value = cursor
    adapter = MagicMock()
    adapter._connection = raw
    adapter._handle_exception = MagicMock()

    await dialect._async_ping(adapter)

    assert cursor.execute.call_count == 1
    assert cursor.fetchone.call_count == 0, (
        "extra fetchone() round-trip doubles ping latency without "
        "adding liveness signal; execute alone proves the RTT"
    )
    cursor.close.assert_called_once()
