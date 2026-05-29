"""``_force_close_transport`` propagates ``asyncio.CancelledError`` from the hook so
structured-concurrency parents see the cancel; callers catch and re-raise it externally."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_force_close_transport_propagates_cancelled_error() -> None:
    """A ``CancelledError`` from the sync hook propagates rather than being absorbed."""

    dbapi_conn = MagicMock()
    dbapi_conn.address = "host-cancel.cluster:9999"
    dbapi_conn.force_close_transport.side_effect = asyncio.CancelledError()

    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = dbapi_conn

    with pytest.raises(asyncio.CancelledError):
        adapted._force_close_transport()


def test_force_close_transport_still_absorbs_non_cancel_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Non-cancel Exception subclasses from the hook are still absorbed and DEBUG-logged."""

    dbapi_conn = MagicMock()
    dbapi_conn.address = "host-osfail.cluster:9999"
    dbapi_conn.force_close_transport.side_effect = OSError("broken pipe")

    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = dbapi_conn

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        adapted._force_close_transport()

    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "best-effort sync close raised" in msgs, msgs
    assert "OSError" in msgs, msgs
