"""Pin the defensive unexpected-type fallthrough and None-handling in the
Date/Time/DateTime result-processor closures (tightening either into a
TypeError or adding preprocessing before the None-check changes behaviour)."""

from __future__ import annotations

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


class TestDateTimeResultProcessor:
    def test_processor_passes_through_unexpected_type_unchanged(self) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        sentinel = 42
        assert proc(sentinel) == sentinel


class TestDateResultProcessor:
    def test_processor_returns_none_for_none(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        assert proc(None) is None

    def test_processor_passes_through_unexpected_type_unchanged(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        sentinel = 42
        assert proc(sentinel) == sentinel


class TestTimeResultProcessor:
    def test_processor_passes_through_unexpected_type_unchanged(self) -> None:
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        sentinel = 42
        assert proc(sentinel) == sentinel
