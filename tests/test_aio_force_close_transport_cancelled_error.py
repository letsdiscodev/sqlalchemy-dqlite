"""``AsyncAdaptedConnection._force_close_transport`` suppression
covers ``asyncio.CancelledError`` alongside ``Exception``.

The package's documented close-discipline (see ``aio.py:307``'s
sibling cursor-close suppression) is to use the
``(Exception, asyncio.CancelledError)`` tuple so a greenlet-level
cancel is absorbed by the best-effort teardown but
``KeyboardInterrupt`` / ``SystemExit`` propagate. The
``_force_close_transport`` site was previously bare ``Exception``,
which let a ``CancelledError`` from the sync ``force_close_transport``
hook escape during pool dispose — pin the symmetric behaviour.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_force_close_transport_suppresses_cancelled_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``CancelledError`` raised by the sync hook is swallowed and
    logged at DEBUG, matching the cursor-close suppression discipline.
    """

    dbapi_conn = MagicMock()
    dbapi_conn.address = "host-cancel.cluster:9999"
    dbapi_conn.force_close_transport.side_effect = asyncio.CancelledError()

    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = dbapi_conn

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        # Must NOT raise.
        adapted._force_close_transport()

    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "best-effort sync close raised" in msgs, msgs
    assert "CancelledError" in msgs, msgs
