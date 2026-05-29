"""SA bind processors reject cross-type payloads symmetrically with the
result-side, so a bind→read round-trip can't write a cell the reader
rejects. ``DataError`` (PEP 249) matches the result-side class."""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime


class TestDqliteDateTimeBindRejectsTimeOnlyPayload:
    def test_time_only_payload_raises_data_error(self) -> None:
        proc = _DqliteDateTime(timezone=False).bind_processor(None)
        assert proc is not None
        with pytest.raises(DataError, match="time-only|datetime.time"):
            proc(datetime.time(12, 30, 0))

    def test_aware_time_raises_data_error(self) -> None:
        proc = _DqliteDateTime(timezone=True).bind_processor(None)
        assert proc is not None
        with pytest.raises(DataError):
            proc(datetime.time(12, 30, 0, tzinfo=datetime.UTC))

    def test_well_formed_datetime_still_passes(self) -> None:
        """Formatted with six fractional digits so cross-writer
        literal-string predicates match pysqlite bit-identically (the
        dbapi encoder omits the suffix when microseconds are zero)."""
        proc = _DqliteDateTime(timezone=False).bind_processor(None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert proc(dt) == "2024-01-02 03:04:05.000000"

    def test_bare_date_still_widens_to_midnight(self) -> None:
        """A bare date widens to midnight with explicit ``.000000`` so
        cross-writer literal-string predicates match pysqlite."""
        proc = _DqliteDateTime(timezone=False).bind_processor(None)
        assert proc is not None
        d = datetime.date(2024, 1, 2)
        assert proc(d) == "2024-01-02 00:00:00.000000"


class TestDqliteDateBindNarrowsDatetimeAndRejectsTime:
    def test_datetime_payload_narrows_to_date(self) -> None:
        """A ``datetime`` bound to a Date column narrows to
        ``YYYY-MM-DD`` (pysqlite-parity)."""
        proc = _DqliteDate().bind_processor(None)
        assert proc is not None
        dt = datetime.datetime(2020, 5, 17, 14, 30, 0)
        result = proc(dt)
        assert result == datetime.date(2020, 5, 17)
        assert isinstance(result, datetime.date)
        # date is NOT a subclass of datetime, so the narrow is observable.
        assert not isinstance(result, datetime.datetime)

    def test_time_only_payload_raises_data_error(self) -> None:
        proc = _DqliteDate().bind_processor(None)
        assert proc is not None
        with pytest.raises(DataError, match="time-only|datetime.time"):
            proc(datetime.time(12, 30, 0))

    def test_well_formed_date_still_passes(self) -> None:
        proc = _DqliteDate().bind_processor(None)
        assert proc is not None
        d = datetime.date(2020, 5, 17)
        assert proc(d) == d

    def test_none_pass_through(self) -> None:
        proc = _DqliteDate().bind_processor(None)
        assert proc is not None
        assert proc(None) is None
