"""Pin: ``_DqliteTime.bind_processor`` raises ``DataError`` on
cross-type ``datetime.datetime`` / ``datetime.date`` payloads and
emits the six-digit microsecond suffix for valid ``datetime.time``
values.

Sibling discipline parity with ``_DqliteDateTime`` and ``_DqliteDate``
which both raise ``DataError`` on the symmetric cross-type confusion
to avoid the round-trip-self-rejection fork (the dialect's reader
would otherwise raise on cells the same dialect's writer wrote).
And cross-writer parity with pysqlite's
``TIME._storage_format = "%H:%M:%S.%(microsecond)06d"`` which always
emits the six-zero fractional suffix: without the override a
``time(12, 30, 0)`` reached ``_iso8601_from_time`` which omits the
suffix when ``microsecond == 0``, breaking literal-string predicates
against pysqlite-written cells.
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import DataError
from sqlalchemydqlite.base import _DqliteTime


def _bind() -> Any:
    return _DqliteTime().bind_processor(MagicMock())


def test_bind_datetime_payload_rejected() -> None:
    """A ``datetime.datetime`` bound to a ``Time`` column is rejected
    at bind so the dialect's bind+read round-trip does not produce a
    cell its own reader narrows lossily."""
    with pytest.raises(DataError, match="datetime"):
        _bind()(datetime.datetime(2021, 3, 15, 12, 30, 45))


def test_bind_date_payload_rejected() -> None:
    """A ``datetime.date`` bound to a ``Time`` column is rejected at
    bind — a date has no time component."""
    with pytest.raises(DataError, match="date"):
        _bind()(datetime.date(2021, 3, 15))


def test_bind_time_zero_microseconds_emits_six_zeros() -> None:
    """Pysqlite parity: ``time(12, 30, 0)`` formats as
    ``"12:30:00.000000"`` (always-on six fractional digits)."""
    out = _bind()(datetime.time(12, 30, 0))
    assert out == "12:30:00.000000", out


def test_bind_time_nonzero_microseconds_preserves_suffix() -> None:
    """Existing behaviour: ``microsecond != 0`` still gets the six
    fractional digits — now via the explicit format path."""
    out = _bind()(datetime.time(12, 30, 0, 123456))
    assert out == "12:30:00.123456", out


def test_bind_aware_time_emits_offset_suffix() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=2))
    out = _bind()(datetime.time(12, 30, 0, tzinfo=tz))
    assert out == "12:30:00.000000+02:00", out


def test_bind_none_passes_through() -> None:
    assert _bind()(None) is None
