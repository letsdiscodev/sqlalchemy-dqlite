"""Pin: async ``DqliteDialect_aio.do_terminate`` suppresses tail
``Exception`` from ``dbapi_connection.terminate()`` and emits a debug
log with peer + id, mirroring the sync sibling at
``DqliteDialect.do_terminate``.

SA's ``has_terminate=True`` contract promises a non-raising path on
``engine.dispose()``. ``CancelledError`` must NOT be swallowed —
asyncio's structured-concurrency contract says cancels propagate.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio


def test_do_terminate_suppresses_runtime_error() -> None:
    """A tail ``Exception`` from ``terminate()`` must not propagate."""
    conn = MagicMock()
    conn.terminate.side_effect = RuntimeError("transport gone")

    DqliteDialect_aio().do_terminate(conn)
    conn.terminate.assert_called_once_with()


def test_do_terminate_log_includes_peer_and_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = MagicMock()
    conn.address = "host-7.cluster:9001"
    conn.terminate.side_effect = RuntimeError("transport gone")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        DqliteDialect_aio().do_terminate(conn)

    formatted_messages = [record.getMessage() for record in caplog.records]
    combined = "\n".join(formatted_messages)
    assert "peer=" in combined, formatted_messages
    assert "id=" in combined, formatted_messages
    assert "host-7.cluster:9001" in combined, formatted_messages
    assert str(id(conn)) in combined, formatted_messages


def test_do_terminate_propagates_cancellederror() -> None:
    """``asyncio.CancelledError`` must propagate — asyncio
    structured-concurrency wins over SA's non-raising promise here.
    """
    conn = MagicMock()
    conn.terminate.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        DqliteDialect_aio().do_terminate(conn)


def test_do_terminate_clean_path_unchanged() -> None:
    """Regression guard: ``terminate()`` returning normally is still a
    no-raise no-log path.
    """
    conn = MagicMock()
    DqliteDialect_aio().do_terminate(conn)
    conn.terminate.assert_called_once_with()
