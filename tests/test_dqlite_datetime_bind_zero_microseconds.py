"""Pin: ``_DqliteDateTime.bind_processor`` always emits six fractional digits, even when
``microsecond == 0``, matching pysqlite so cross-writer literal-string predicates match."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteDateTime


def _bind() -> Any:
    return _DqliteDateTime().bind_processor(MagicMock())


def test_naive_datetime_zero_microseconds_emits_six_zeros() -> None:
    out = _bind()(datetime.datetime(2021, 3, 15, 12, 5, 57))
    assert out == "2021-03-15 12:05:57.000000", out


def test_naive_datetime_nonzero_microseconds_unchanged() -> None:
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, 123456)
    out = _bind()(value)
    assert out == "2021-03-15 12:05:57.123456", out


def test_widen_branch_date_unchanged() -> None:
    out = _bind()(datetime.date(2021, 3, 15))
    assert out == "2021-03-15 00:00:00.000000", out


def test_aware_datetime_zero_microseconds_carries_offset() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=2))
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, tzinfo=tz)
    out = _bind()(value)
    assert out == "2021-03-15 12:05:57.000000+02:00", out


def test_aware_datetime_nonzero_microseconds_carries_offset() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, 123456, tzinfo=tz)
    out = _bind()(value)
    assert out == "2021-03-15 12:05:57.123456-05:00", out


def test_none_passes_through() -> None:
    assert _bind()(None) is None
