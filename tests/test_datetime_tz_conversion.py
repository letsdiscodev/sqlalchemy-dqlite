"""``_DqliteDateTime(timezone=False)`` must convert tz-aware inputs to UTC
before dropping tzinfo.

The old bare ``replace(tzinfo=None)`` silently dropped the offset while
keeping the local-offset wall-clock digits, shifting the stored instant
by the offset magnitude. ``DateTime(timezone=False)`` is documented as
"naive wall-clock interpreted as UTC" — an aware input therefore must
be normalised through UTC first.

Complement: ``timezone=True`` attaches UTC to naive inputs so the ORM
contract's aware-return guarantee holds for cells stored by writers
that omit a tz suffix.
"""

from __future__ import annotations

import datetime

from sqlalchemydqlite.base import _DqliteDateTime


class TestDateTimeTimezoneFalse:
    def test_non_utc_aware_input_is_converted_to_utc_naive(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        plus_530 = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        v = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=plus_530)
        # UTC instant is 06:30 — NOT naive 12:00.
        assert proc(v) == datetime.datetime(2024, 1, 1, 6, 30, 0)

    def test_utc_aware_input_is_unchanged_after_naive_narrowing(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        v = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        assert proc(v) == datetime.datetime(2024, 1, 1, 12, 0, 0)

    def test_naive_input_passes_through_unchanged(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        v = datetime.datetime(2024, 1, 1, 12, 0, 0)
        assert proc(v) == v
        assert proc(v).tzinfo is None


class TestDateTimeTimezoneTrue:
    def test_naive_input_is_promoted_to_utc_aware(self) -> None:
        proc = _DqliteDateTime(timezone=True).result_processor(None, None)
        assert proc is not None
        v = datetime.datetime(2024, 1, 1, 12, 0, 0)
        out = proc(v)
        assert out == datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        assert out.tzinfo is datetime.UTC

    def test_aware_input_preserves_offset(self) -> None:
        proc = _DqliteDateTime(timezone=True).result_processor(None, None)
        assert proc is not None
        plus_530 = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        v = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=plus_530)
        assert proc(v) == v
