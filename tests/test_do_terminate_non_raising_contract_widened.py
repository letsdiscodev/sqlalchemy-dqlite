"""Pin: ``DqliteDialect.do_terminate`` and
``DqliteDialect_aio.do_terminate`` honour SA's ``has_terminate=True``
non-raising pool contract for unexpected ``Exception`` shapes.

The narrow ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` catch absorbed expected
transport-class failures (OSError + dbapi.Error subclasses +
DqliteConnectionError + RuntimeError + ReferenceError) but let
``AttributeError`` / ``TypeError`` / ``NotImplementedError`` — the
canonical cross-repo dbapi refactor regression shapes — propagate
and abort SA's pool finalize / ``engine.dispose()``. SA's
``has_terminate=True`` contract is binary non-raising; widen the
catch to absorb all ``Exception`` subclasses but emit a WARNING
(not DEBUG) so refactor regressions stay loudly visible. KeyboardInterrupt
/ SystemExit (and the async-sibling's CancelledError) remain
uncaught — they signal cooperative shutdown.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect

# --- sync dialect ---------------------------------------------------------


def test_sync_do_terminate_absorbs_attribute_error_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A cross-repo dbapi refactor that removed ``force_close_transport``
    surfaces as ``AttributeError``; ``do_terminate`` must absorb it
    (SA non-raising contract) but emit a WARNING so the regression
    is visible in operator logs."""
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
    """Existing behaviour: an expected transport-class failure is
    DEBUG-logged (NOT WARNING) and absorbed."""
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
    """``BaseException`` shapes (KeyboardInterrupt / SystemExit) must
    propagate so signal-handler-driven dispose sees the interrupt."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    conn.address = "host.cluster:9999"
    conn.force_close_transport.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        dialect.do_terminate(conn)


# --- async dialect --------------------------------------------------------


def test_async_do_terminate_absorbs_attribute_error_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Symmetric: async sibling honours the same SA contract."""
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
