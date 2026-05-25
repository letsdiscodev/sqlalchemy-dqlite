"""``AsyncAdaptedConnection._force_close_transport`` propagates
``asyncio.CancelledError`` from the hook so structured-concurrency
parents see the cancel.

The catch was previously ``(Exception, asyncio.CancelledError)``,
which absorbed cancellation inside the helper. An outer
``asyncio.TaskGroup`` / ``asyncio.timeout`` /
``anyio.create_task_group`` parent that cancelled a child during
``engine.dispose()`` could then hang on its ``__aexit__`` waiting
for a cancel-acknowledgement that never arrived because the child
returned normally. The close-arm and terminate-arm callers catch
``CancelledError`` externally, run the force-close, and then
re-raise; the helper itself does NOT need to absorb cancel for
those sites to work correctly.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_force_close_transport_propagates_cancelled_error() -> None:
    """A ``CancelledError`` raised by the sync hook propagates so
    structured-concurrency parents see the cancel. Sibling close /
    terminate callers catch ``CancelledError`` externally and re-raise
    after invoking this helper; absorbing here would corrupt that
    contract."""

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
    """Non-cancel Exception subclasses from the hook are still
    absorbed and DEBUG-logged — the ``has_terminate=True`` /
    pool-dispose discipline relies on this best-effort behaviour
    for OSError / OperationalError shapes."""

    dbapi_conn = MagicMock()
    dbapi_conn.address = "host-osfail.cluster:9999"
    dbapi_conn.force_close_transport.side_effect = OSError("broken pipe")

    adapted = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapted._connection = dbapi_conn

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        # Must NOT raise.
        adapted._force_close_transport()

    msgs = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "best-effort sync close raised" in msgs, msgs
    assert "OSError" in msgs, msgs
