"""Date/Time/DateTime bind, literal and result processors: ISO text, tz, cross-type payloads."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Date, DateTime, Time
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite import base as sqlite_base
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite

import dqlitedbapi.exceptions as dbapi_exc
from dqlitedbapi.exceptions import DataError
from sqlalchemydqlite import DqliteDialect
from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime

UTC = datetime.UTC
PLUS2 = datetime.timezone(datetime.timedelta(hours=2))
NAIVE = datetime.datetime(2024, 3, 5, 6, 7, 8, 9)
AWARE = NAIVE.replace(tzinfo=PLUS2)


def _bind(type_: Any) -> Any:
    return type_.dialect_impl(DqliteDialect()).bind_processor(DqliteDialect())


def _result(type_: Any) -> Any:
    return type_.dialect_impl(DqliteDialect()).result_processor(DqliteDialect(), None)


def _literal(type_: Any) -> Any:
    return type_.dialect_impl(DqliteDialect()).literal_processor(DqliteDialect())


def test_datetime_bind_uses_six_digit_fraction_and_offset() -> None:
    bind = _bind(DateTime())
    assert bind(NAIVE) == "2024-03-05 06:07:08.000009"
    assert bind(NAIVE.replace(microsecond=0)) == "2024-03-05 06:07:08.000000"
    assert bind(AWARE) == "2024-03-05 06:07:08.000009+02:00"
    assert bind(datetime.date(2024, 3, 5)) == "2024-03-05 00:00:00.000000"
    assert bind(None) is None
    with pytest.raises(dbapi_exc.DataError):
        bind(datetime.time(1, 2))
    assert _literal(DateTime())(NAIVE) == "'2024-03-05 06:07:08.000009'"
    assert _literal(DateTime())(None) == "NULL"


def test_datetime_result_accepts_text_and_objects() -> None:
    naive = _result(DateTime())
    assert naive("2024-03-05 06:07:08.000009") == NAIVE
    assert naive("2024-03-05 06:07:08") == NAIVE.replace(microsecond=0)
    assert naive(NAIVE) == NAIVE
    assert naive(AWARE) == NAIVE - datetime.timedelta(hours=2)
    assert naive(None) is None
    assert naive("not a date") == "not a date"
    aware = _result(DateTime(timezone=True))
    assert aware(NAIVE) == NAIVE.replace(tzinfo=UTC)
    assert aware("2024-03-05 06:07:08+02:00") == AWARE.replace(microsecond=0)
    with pytest.raises(dbapi_exc.DataError):
        naive(datetime.time(1, 2))


def test_date_processors() -> None:
    bind, result = _bind(Date()), _result(Date())
    assert bind(datetime.date(2024, 3, 5)) == datetime.date(2024, 3, 5)
    assert bind(NAIVE) == datetime.date(2024, 3, 5)
    with pytest.raises(dbapi_exc.DataError):
        bind(datetime.time(1))
    assert result("2024-03-05") == datetime.date(2024, 3, 5)
    assert result(NAIVE) == datetime.date(2024, 3, 5)
    assert result("garbage") == "garbage"
    assert result(None) is None


def test_time_processors() -> None:
    bind, result, literal = _bind(Time()), _result(Time()), _literal(Time())
    t = datetime.time(6, 7, 8, 9)
    assert bind(t) == "06:07:08.000009"
    assert bind(t.replace(tzinfo=PLUS2)) == "06:07:08.000009+02:00"
    assert literal(t) == "'06:07:08.000009'"
    with pytest.raises(dbapi_exc.DataError):
        bind(NAIVE)
    assert result("06:07:08.000009") == t
    assert result(t) == t
    assert result(NAIVE) == t
    assert result(t.replace(tzinfo=PLUS2)) == t
    assert _result(Time(timezone=True))(t) == t.replace(tzinfo=UTC)
    assert result("garbage") == "garbage"


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


def _datetime_bind() -> Any:
    return _DqliteDateTime().bind_processor(MagicMock())


def test_naive_datetime_zero_microseconds_emits_six_zeros() -> None:
    out = _datetime_bind()(datetime.datetime(2021, 3, 15, 12, 5, 57))
    assert out == "2021-03-15 12:05:57.000000", out


def test_naive_datetime_nonzero_microseconds_unchanged() -> None:
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, 123456)
    out = _datetime_bind()(value)
    assert out == "2021-03-15 12:05:57.123456", out


def test_widen_branch_date_unchanged() -> None:
    out = _datetime_bind()(datetime.date(2021, 3, 15))
    assert out == "2021-03-15 00:00:00.000000", out


def test_aware_datetime_zero_microseconds_carries_offset() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=2))
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, tzinfo=tz)
    out = _datetime_bind()(value)
    assert out == "2021-03-15 12:05:57.000000+02:00", out


def test_aware_datetime_nonzero_microseconds_carries_offset() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, 123456, tzinfo=tz)
    out = _datetime_bind()(value)
    assert out == "2021-03-15 12:05:57.123456-05:00", out


def test_none_passes_through() -> None:
    assert _datetime_bind()(None) is None


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


def _time_bind() -> Any:
    return _DqliteTime().bind_processor(MagicMock())


def test_bind_datetime_payload_rejected() -> None:
    with pytest.raises(DataError, match="datetime"):
        _time_bind()(datetime.datetime(2021, 3, 15, 12, 30, 45))


def test_bind_date_payload_rejected() -> None:
    with pytest.raises(DataError, match="date"):
        _time_bind()(datetime.date(2021, 3, 15))


def test_bind_time_zero_microseconds_emits_six_zeros() -> None:
    out = _time_bind()(datetime.time(12, 30, 0))
    assert out == "12:30:00.000000", out


def test_bind_time_nonzero_microseconds_preserves_suffix() -> None:
    out = _time_bind()(datetime.time(12, 30, 0, 123456))
    assert out == "12:30:00.123456", out


def test_bind_aware_time_emits_offset_suffix() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=2))
    out = _time_bind()(datetime.time(12, 30, 0, tzinfo=tz))
    assert out == "12:30:00.000000+02:00", out


def test_time_bind_none_passes_through() -> None:
    assert _time_bind()(None) is None


def test_bind_processor_returns_callable_for_default_dialect() -> None:
    dialect: Any = MagicMock()
    proc = _DqliteTime().bind_processor(dialect)
    assert callable(proc)


def test_bind_processor_returns_callable_for_timezone_true_variant() -> None:
    dialect: Any = MagicMock()
    proc = _DqliteTime(timezone=True).bind_processor(dialect)
    assert callable(proc)


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


def _make_proc(*, timezone: bool) -> Any:
    return _DqliteTime(timezone=timezone).result_processor(MagicMock(), None)


def test_aware_datetime_payload_preserves_source_offset() -> None:
    proc = _make_proc(timezone=True)
    minus7 = datetime.timezone(datetime.timedelta(hours=-7))
    payload = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=minus7)
    result = proc(payload)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is not None
    assert result.utcoffset() == datetime.timedelta(hours=-7), (
        f"expected -7h offset; got {result.utcoffset()!r}"
    )


def test_naive_datetime_payload_still_attaches_utc_for_timezone_true() -> None:
    proc = _make_proc(timezone=True)
    payload = datetime.datetime(2021, 3, 15, 12, 30, 45)
    result = proc(payload)
    assert isinstance(result, datetime.time)
    assert result.utcoffset() == datetime.timedelta(0)


def test_timezone_false_strips_aware_datetime_tzinfo() -> None:
    proc = _make_proc(timezone=False)
    minus7 = datetime.timezone(datetime.timedelta(hours=-7))
    payload = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=minus7)
    result = proc(payload)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is None


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


def test_colspec_maps_time_to_dqlite_time() -> None:
    assert DqliteDialect.colspecs[sqltypes.Time] is _DqliteTime


def test_result_processor_passes_native_time_through() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    t = datetime.time(12, 5, 57, 105580)
    assert proc(t) is t


def test_result_processor_parses_iso8601_string() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    assert proc("12:05:57.105580") == datetime.time(12, 5, 57, 105580)


def test_result_processor_passes_through_none() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    assert proc(None) is None


def test_result_processor_passes_through_unparseable_string() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    assert proc("not a time") == "not a time"


def test_pysqlite_colspecs_inherits_sqlite_time_colspec() -> None:
    assert sqltypes.Time in SQLiteDialect_pysqlite.colspecs, (
        "pysqlite inherits sqltypes.Time: TIME from SQLiteDialect.colspecs"
    )
    assert SQLiteDialect_pysqlite.colspecs[sqltypes.Time] is sqlite_base.TIME


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
