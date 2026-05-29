"""Pin: ``_DqliteTime.bind_processor`` raises ``DataError`` on cross-type datetime/date
payloads and always emits the six-digit microsecond suffix for ``datetime.time``
(pysqlite parity; the encoder omits it at zero)."""

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
    with pytest.raises(DataError, match="datetime"):
        _bind()(datetime.datetime(2021, 3, 15, 12, 30, 45))


def test_bind_date_payload_rejected() -> None:
    with pytest.raises(DataError, match="date"):
        _bind()(datetime.date(2021, 3, 15))


def test_bind_time_zero_microseconds_emits_six_zeros() -> None:
    out = _bind()(datetime.time(12, 30, 0))
    assert out == "12:30:00.000000", out


def test_bind_time_nonzero_microseconds_preserves_suffix() -> None:
    out = _bind()(datetime.time(12, 30, 0, 123456))
    assert out == "12:30:00.123456", out


def test_bind_aware_time_emits_offset_suffix() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=2))
    out = _bind()(datetime.time(12, 30, 0, tzinfo=tz))
    assert out == "12:30:00.000000+02:00", out


def test_bind_none_passes_through() -> None:
    assert _bind()(None) is None
