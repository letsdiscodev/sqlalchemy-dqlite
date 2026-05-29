"""Pin: _DqliteTime.result_processor honours Time(timezone=...) (attach/strip tzinfo).
Bind side is intentionally not extended: pysqlite renders Time without offset, so only the
result-side normalisation is observable."""

from __future__ import annotations

import datetime

from sqlalchemydqlite.base import _DqliteTime


class TestDqliteTimeResultProcessorTimezone:
    def test_timezone_true_attaches_utc_to_naive_value(self) -> None:
        proc = _DqliteTime(timezone=True).result_processor(None, None)
        assert proc is not None
        result = proc(datetime.time(12, 30, 45))
        assert result == datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert result.tzinfo is not None

    def test_timezone_true_preserves_aware_value_unchanged(self) -> None:
        proc = _DqliteTime(timezone=True).result_processor(None, None)
        assert proc is not None
        aware = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert proc(aware) == aware

    def test_timezone_false_strips_aware_tzinfo(self) -> None:
        proc = _DqliteTime(timezone=False).result_processor(None, None)
        assert proc is not None
        aware = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        result = proc(aware)
        assert result == datetime.time(12, 30, 45)
        assert result.tzinfo is None

    def test_timezone_false_preserves_naive_value_unchanged(self) -> None:
        proc = _DqliteTime(timezone=False).result_processor(None, None)
        assert proc is not None
        naive = datetime.time(12, 30, 45)
        assert proc(naive) == naive

    def test_default_timezone_is_false_strips_tzinfo(self) -> None:
        """Time() defaults to timezone=False (matching SA's sqltypes.Time default)."""
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        aware = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        result = proc(aware)
        assert result.tzinfo is None
