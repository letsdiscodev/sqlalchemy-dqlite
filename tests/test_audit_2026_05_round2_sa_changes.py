"""Behavioural pins for the second 2026-05 SA-side audit round."""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import NotSupportedError, ProgrammingError
from sqlalchemydqlite.aio import (
    AsyncAdaptedConnection,
    AsyncAdaptedCursor,
    DqliteDialect_aio,
)
from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


def test_async_adapted_connection_exposes_dbapi_attribute() -> None:
    """SA reference parity: ``AsyncAdaptedConnection(dbapi, connection)`` exposes
    a ``dbapi`` attribute that third-party introspection hard-getattrs."""
    sentinel = MagicMock(name="dbapi_module")
    adapter = AsyncAdaptedConnection(sentinel, MagicMock())
    assert adapter.dbapi is sentinel


def test_async_adapted_connection_dbapi_kwarg_optional() -> None:
    """Legacy call sites passing only the connection still work; ``dbapi`` defaults
    to None."""
    adapter = AsyncAdaptedConnection(MagicMock())
    assert adapter.dbapi is None


def test_async_adapted_cursor_rownumber_raises_notsupported() -> None:
    """Reading rownumber raises NotSupportedError (a dbapi.Error), not AttributeError."""
    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    with pytest.raises(NotSupportedError, match="rownumber"):
        _ = cur.rownumber


def test_async_adapted_cursor_rownumber_on_closed_raises_interfaceerror() -> None:
    """Closed-cursor guard fires before NotSupportedError."""
    from dqlitedbapi.exceptions import InterfaceError

    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    cur.close()
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _ = cur.rownumber


def test_async_adapted_cursor_scroll_invalid_mode_raises_programming_error() -> None:
    """PEP 249 §6.1.1: scroll validates ``mode`` before the unconditional
    NotSupportedError, so caller typos surface as ProgrammingError."""
    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    with pytest.raises(ProgrammingError, match="must be 'relative' or 'absolute'"):
        cur.scroll(0, mode="absolutely")


def test_async_adapted_cursor_scroll_valid_mode_raises_notsupported() -> None:
    """Positive control: valid modes raise NotSupportedError (cursors aren't scrollable)."""
    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    with pytest.raises(NotSupportedError):
        cur.scroll(0, mode="relative")
    with pytest.raises(NotSupportedError):
        cur.scroll(0, mode="absolute")


@pytest.mark.parametrize("code", [10250, 10506])
def test_is_disconnect_classifies_extended_leader_codes(code: int) -> None:
    """Extended leader-flip codes classify as disconnect via the code-based arm;
    primary-masking the code on the exception would silently break detection."""
    from dqlitedbapi.exceptions import OperationalError

    dialect = DqliteDialect_aio()
    err = OperationalError("simulated", code=code)
    assert dialect.is_disconnect(err, connection=None, cursor=None) is True


def test_dqlite_datetime_result_processor_passes_none() -> None:
    proc = _DqliteDateTime().result_processor(MagicMock(), None)
    assert proc is not None
    assert proc(None) is None


def test_dqlite_time_result_processor_passes_none() -> None:
    proc = _DqliteTime().result_processor(MagicMock(), None)
    assert proc is not None
    assert proc(None) is None


def test_dqlite_date_result_processor_passes_none() -> None:
    proc = _DqliteDate().result_processor(MagicMock(), None)
    assert proc is not None
    assert proc(None) is None
