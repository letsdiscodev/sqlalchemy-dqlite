"""Pin: _DqliteTime narrows datetime to time via .timetz(), then re-attaches UTC
only when the narrowed time is naive (timezone=True)."""

from __future__ import annotations

import datetime

from sqlalchemydqlite.base import _DqliteTime


class TestDqliteTimeCrossAxisNarrowAndAttachUtc:
    def test_naive_datetime_narrows_then_attaches_utc(self) -> None:
        proc = _DqliteTime(timezone=True).result_processor(None, None)
        assert proc is not None
        result = proc(datetime.datetime(2024, 1, 2, 12, 30, 45))
        assert result == datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert isinstance(result, datetime.time)
        assert not isinstance(result, datetime.datetime)

    def test_aware_datetime_narrows_preserves_source_offset(self) -> None:
        """An aware datetime keeps its offset; the UTC re-attach fires only when naive."""
        plus5 = datetime.timezone(datetime.timedelta(hours=5))
        aware_dt = datetime.datetime(2024, 1, 2, 12, 30, 45, tzinfo=plus5)
        proc = _DqliteTime(timezone=True).result_processor(None, None)
        assert proc is not None
        result = proc(aware_dt)
        assert isinstance(result, datetime.time)
        assert not isinstance(result, datetime.datetime)
        # Source offset preserved — NOT silently rewritten to UTC.
        assert result.utcoffset() == datetime.timedelta(hours=5)
