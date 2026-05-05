"""Behavioural pins for the second 2026-05 SA-side audit round.

- ``AsyncAdaptedConnection`` exposes a ``dbapi`` attribute mirroring
  SA's reference connector. Third-party instrumentation
  (Sentry / Datadog wrappers) hard-getattrs this to reach the dbapi
  module's exception classes.
- ``AsyncAdaptedCursor.rownumber`` raises ``NotSupportedError``
  rather than ``AttributeError`` (which would escape the
  ``dbapi.Error`` hierarchy).
- ``AsyncAdaptedCursor.scroll`` validates ``mode`` BEFORE the
  unconditional NotSupportedError, surfacing caller typos as
  ``ProgrammingError`` (PEP 249 §6.1.1).
- ``DqliteDialect_aio`` is_disconnect classifies an
  ``OperationalError`` carrying ``code=SQLITE_IOERR_NOT_LEADER``
  (10250) or ``SQLITE_IOERR_LEADERSHIP_LOST`` (10506) as
  disconnect via the extended-code arm — pin against silent
  regression that primary-masks the code on the exception.
- ``_DqliteDateTime`` / ``_DqliteTime`` ``result_processor`` closures
  pass ``None`` through unchanged.
"""

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
    """SA reference connector parity: AsyncAdaptedConnection has
    a ``dbapi`` attribute. Third-party introspection hard-getattrs
    it to reach the dbapi module's exception classes."""
    sentinel = MagicMock(name="dbapi_module")
    adapter = AsyncAdaptedConnection(MagicMock(), dbapi=sentinel)
    assert adapter.dbapi is sentinel


def test_async_adapted_connection_dbapi_kwarg_optional() -> None:
    """Backward compatibility: existing call sites that pass only
    the connection (single positional arg) still work; ``dbapi``
    defaults to None."""
    adapter = AsyncAdaptedConnection(MagicMock())
    assert adapter.dbapi is None


def test_async_adapted_cursor_rownumber_raises_notsupported() -> None:
    """Pin: rownumber is not a counter; reading it raises
    NotSupportedError (a dbapi.Error) rather than AttributeError."""
    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    with pytest.raises(NotSupportedError, match="rownumber"):
        _ = cur.rownumber


def test_async_adapted_cursor_rownumber_on_closed_raises_interfaceerror() -> None:
    """Closed-cursor guard fires before NotSupportedError so the
    generic stale-cursor diagnostic is preserved."""
    from dqlitedbapi.exceptions import InterfaceError

    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    cur.close()
    with pytest.raises(InterfaceError, match="cursor is closed"):
        _ = cur.rownumber


def test_async_adapted_cursor_scroll_invalid_mode_raises_programming_error() -> None:
    """PEP 249 §6.1.1: scroll(value, mode) validator. Caller typos
    surface as ProgrammingError BEFORE the unconditional
    NotSupportedError so cross-driver code catches them via
    ``except ProgrammingError:``."""
    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    with pytest.raises(ProgrammingError, match="must be 'relative' or 'absolute'"):
        cur.scroll(0, mode="absolutely")


def test_async_adapted_cursor_scroll_valid_mode_raises_notsupported() -> None:
    """Positive control: valid modes raise NotSupportedError
    (dqlite cursors are not scrollable)."""
    adapter = AsyncAdaptedConnection(MagicMock())
    cur = AsyncAdaptedCursor(adapter)
    with pytest.raises(NotSupportedError):
        cur.scroll(0, mode="relative")
    with pytest.raises(NotSupportedError):
        cur.scroll(0, mode="absolute")


@pytest.mark.parametrize("code", [10250, 10506])
def test_is_disconnect_classifies_extended_leader_codes(code: int) -> None:
    """Pin: the extended SQLite codes SQLITE_IOERR_NOT_LEADER /
    SQLITE_IOERR_LEADERSHIP_LOST classify as disconnect via the
    code-based arm. A future helper that "helpfully" primary-masks
    the code on the exception attribute would silently break leader-
    flip detection — this pin guards the masking-invariant."""
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
