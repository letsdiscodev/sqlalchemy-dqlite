"""Pin: sync ``DqliteDialect.do_terminate``'s debug log includes
``peer=...`` and ``id=...`` substrings, mirroring the async sibling at
``aio.py:1216-1298``.

A flapping leader produces repeated terminate-log lines. Without peer
and id, the operator cannot correlate which slot/node went away.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.base import DqliteDialect


def test_do_terminate_log_includes_peer_and_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = MagicMock()
    conn.address = "host-3.cluster:9001"
    conn.force_close_transport.side_effect = RuntimeError("transport gone")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        DqliteDialect().do_terminate(conn)

    formatted_messages = [record.getMessage() for record in caplog.records]
    combined = "\n".join(formatted_messages)
    assert "peer=" in combined, formatted_messages
    assert "id=" in combined, formatted_messages
    assert "host-3.cluster:9001" in combined, formatted_messages
    assert str(id(conn)) in combined, formatted_messages
