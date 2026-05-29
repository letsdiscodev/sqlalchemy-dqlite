"""bind_processor widens datetime.date to a midnight datetime, matching
pysqlite so a sibling writer stores the full-timestamp shape."""

from __future__ import annotations

import datetime

from sqlalchemydqlite.base import _DqliteDateTime


def test_bind_date_widens_to_midnight_datetime_with_microseconds() -> None:
    """A bare date widens to "YYYY-MM-DD 00:00:00.000000"; bind_processor emits
    the string directly because _iso8601_from_datetime drops .000000 at microsecond 0."""
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    widened = proc(datetime.date(2021, 3, 15))
    assert widened == "2021-03-15 00:00:00.000000"


def test_bind_datetime_formats_with_six_fractional_digits() -> None:
    """Naive datetime always emits six fractional digits, even at microsecond 0."""
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    dt = datetime.datetime(2021, 3, 15, 12, 30, 45)
    assert proc(dt) == "2021-03-15 12:30:45.000000"


def test_bind_aware_datetime_formats_with_offset_suffix() -> None:
    """Tz-aware datetime emits six fractional digits plus the ±HH:MM offset suffix."""
    proc = _DqliteDateTime(timezone=True).bind_processor(None)
    assert proc is not None
    tz = datetime.timezone(datetime.timedelta(hours=5))
    dt = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=tz)
    assert proc(dt) == "2021-03-15 12:30:45.000000+05:00"


def test_bind_none_passes_through() -> None:
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    assert proc(None) is None
