"""Pin: ``DqliteDialect_aio._async_ping`` emits a ``logger.debug``
line before raising the closed-proxy ``InterfaceError`` fast-path.

The fast-path triggers when SA's pool calls ``do_ping`` on a
connection whose adapter has already been closed (the weakref-proxy
swap in ``AsyncAdaptedConnection.close``). The ``InterfaceError``
raise is correct — it routes through ``do_ping``'s outer catch tuple
so the pool retires the slot — but without a log line the operator
sees a silent slot retirement and cannot distinguish "wire ping
failed" from "adapter was already closed at checkout". Sibling
close()/terminate()/transport-class arms in the same module emit
DEBUG signal on analogous slot-retirement transitions.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from typing import Any

import pytest

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.aio import DqliteDialect_aio  # noqa: I001


class _ClosedishAdapter:
    """Minimal adapter shape: ``_connection`` is a ``weakref.proxy``
    so the closed-proxy fast-path fires."""

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
