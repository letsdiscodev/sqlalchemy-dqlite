"""Pin: ``do_terminate`` (sync/async) absorbs all ``Exception`` subclasses per SA's
non-raising contract, with a WARNING for unexpected shapes. BaseException (and async
CancelledError) stays uncaught — it signals cooperative shutdown."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_sync_do_terminate_absorbs_attribute_error_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.force_close_transport.side_effect = AttributeError("force_close_transport was renamed")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        dialect.do_terminate(conn)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, (
        f"expected WARNING on unexpected terminate shape; got "
        f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
    )
    assert "do_terminate" in warnings[0].getMessage()


def test_sync_do_terminate_absorbs_type_error_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.force_close_transport.side_effect = TypeError("missing required keyword: 'deadline'")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        dialect.do_terminate(conn)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected WARNING on TypeError from terminate"


def test_sync_do_terminate_expected_transport_class_still_debug_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.force_close_transport.side_effect = OperationalError("transient")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        dialect.do_terminate(conn)

    debugs = [
        r for r in caplog.records if r.levelno == logging.DEBUG and "do_terminate" in r.getMessage()
    ]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert debugs, "expected DEBUG record for expected transport-class shape"
    assert not warnings, "must not emit WARNING for an expected transport-class shape"


def test_sync_do_terminate_keyboard_interrupt_propagates() -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.force_close_transport.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        dialect.do_terminate(conn)


def test_async_do_terminate_absorbs_attribute_error_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.terminate.side_effect = AttributeError("terminate was renamed")

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        dialect.do_terminate(conn)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, (
        f"expected WARNING on unexpected terminate shape; got "
        f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
    )
    assert "do_terminate" in warnings[0].getMessage()


def test_async_do_terminate_keyboard_interrupt_propagates() -> None:
    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.terminate.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        dialect.do_terminate(conn)
