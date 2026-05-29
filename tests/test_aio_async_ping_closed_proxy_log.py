"""``_async_ping`` emits a DEBUG line before raising the closed-proxy ``InterfaceError``
so the operator can tell "already closed at checkout" from a real wire-ping failure."""

from __future__ import annotations

import asyncio
import logging
import weakref
from typing import Any

import pytest

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.aio import DqliteDialect_aio  # noqa: I001


class _ClosedishAdapter:
    """Minimal adapter whose ``_connection`` is a ``weakref.proxy`` (fires the fast-path)."""

    def __init__(self) -> None:
        target: Any = type("Inner", (), {})()
        self._inner_strong_ref = target
        self._connection = weakref.proxy(target)


def test_async_ping_closed_proxy_fastpath_emits_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dialect = DqliteDialect_aio()
    adapter = _ClosedishAdapter()

    with (
        caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"),
        pytest.raises(InterfaceError, match="closed"),
    ):
        asyncio.new_event_loop().run_until_complete(
            dialect._async_ping(adapter),
        )

    closed_records = [
        record
        for record in caplog.records
        if record.levelno == logging.DEBUG
        and "_async_ping" in record.getMessage()
        and "closed" in record.getMessage().lower()
    ]
    assert closed_records, (
        "expected one DEBUG record on the closed-proxy fast-path; "
        f"got records: {[(r.name, r.levelname, r.getMessage()) for r in caplog.records]}"
    )
    assert f"id={id(adapter)}" in closed_records[0].getMessage()
