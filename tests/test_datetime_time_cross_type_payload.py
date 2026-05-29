"""Cross-type payloads from the polymorphic _datetime_from_iso8601: DateTime/Date
columns raise DataError on a time-only value (no date to fabricate); a Time column
narrows a full datetime via .time() (drops the extra dimension)."""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


class TestDqliteDateTimeRejectsTimeOnlyPayload:
    """DateTime column receiving a datetime.time must raise, not pass it through."""

    def test_time_only_payload_raises_data_error(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        with pytest.raises(DataError, match="time-only|datetime.time"):
            proc(datetime.time(12, 30, 0))

    def test_time_with_microseconds_raises_data_error(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        with pytest.raises(DataError):
            proc(datetime.time(12, 30, 0, 500_000))

    def test_aware_time_raises_data_error(self) -> None:
        proc = _DqliteDateTime(timezone=True).result_processor(None, None)
        assert proc is not None
        with pytest.raises(DataError):
            proc(datetime.time(12, 30, 0, tzinfo=datetime.UTC))

    def test_well_formed_datetime_still_passes(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert proc(dt) == dt


class TestDqliteTimeNarrowsDatetimePayload:
    """Time column narrows a datetime.datetime via .time(), mirroring _DqliteDate."""

    def test_datetime_payload_narrows_to_time(self) -> None:
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 12, 30, 45)
        result = proc(dt)
        assert result == datetime.time(12, 30, 45)
        assert isinstance(result, datetime.time)
        assert not isinstance(result, datetime.datetime)

    def test_datetime_with_microseconds_preserves_microseconds(self) -> None:
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 12, 30, 45, 500_000)
        result = proc(dt)
        assert result == datetime.time(12, 30, 45, 500_000)

    def test_aware_datetime_narrows_to_naive_time(self) -> None:
        """datetime.time() drops tzinfo (the tz-preserving analogue is .timetz())."""
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 12, 30, 0, tzinfo=datetime.UTC)
        result = proc(dt)
        assert isinstance(result, datetime.time)
        assert not isinstance(result, datetime.datetime)
        assert result.tzinfo is None

    def test_well_formed_time_still_passes(self) -> None:
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        t = datetime.time(12, 30, 0)
        assert proc(t) == t


class TestDqliteDateRejectsTimeOnlyPayload:
    """Date column receiving a datetime.time must raise (no date to fabricate)."""

    def test_time_only_payload_raises_data_error(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        with pytest.raises(DataError, match="time-only|datetime.time"):
            proc(datetime.time(12, 30, 0))

    def test_well_formed_datetime_still_narrows_to_date(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 12, 30, 0)
        assert proc(dt) == datetime.date(2024, 1, 2)

    def test_well_formed_date_still_passes(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        d = datetime.date(2024, 1, 2)
        assert proc(d) == d
