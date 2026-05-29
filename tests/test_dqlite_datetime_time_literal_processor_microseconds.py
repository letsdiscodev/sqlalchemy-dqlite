"""Pin: ``_DqliteDateTime`` / ``_DqliteTime`` literal_processor always emit the six-digit
microsecond suffix (matching pysqlite); the inherited isoformat path omits it at zero,
breaking cross-writer literal-string predicate matching."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteDateTime, _DqliteTime


def _datetime_literal() -> Any:
    return _DqliteDateTime().literal_processor(MagicMock())


def _time_literal() -> Any:
    return _DqliteTime().literal_processor(MagicMock())


def test_datetime_literal_zero_microseconds_emits_six_zeros() -> None:
    proc = _datetime_literal()
    assert proc is not None
    assert proc(datetime.datetime(2021, 3, 15, 0, 0, 0)) == "'2021-03-15 00:00:00.000000'"
    assert proc(datetime.datetime(2021, 3, 15, 12, 30, 45)) == "'2021-03-15 12:30:45.000000'"


def test_datetime_literal_nonzero_microseconds_unchanged() -> None:
    proc = _datetime_literal()
    assert proc is not None
    out = proc(datetime.datetime(2021, 3, 15, 12, 30, 45, 123456))
    assert out == "'2021-03-15 12:30:45.123456'", out


def test_time_literal_zero_microseconds_emits_six_zeros() -> None:
    proc = _time_literal()
    assert proc is not None
    assert proc(datetime.time(12, 30, 45)) == "'12:30:45.000000'"


def test_time_literal_nonzero_microseconds_unchanged() -> None:
    proc = _time_literal()
    assert proc is not None
    assert proc(datetime.time(12, 30, 45, 123456)) == "'12:30:45.123456'"


def test_datetime_literal_widens_date_to_midnight() -> None:
    proc = _datetime_literal()
    assert proc is not None
    assert proc(datetime.date(2021, 3, 15)) == "'2021-03-15 00:00:00.000000'"
