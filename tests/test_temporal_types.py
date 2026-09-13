"""Date/time processors: ISO 8601 text and dbapi-decoded objects both round-trip."""

from __future__ import annotations

import datetime
from typing import Any

import pytest
from sqlalchemy import Date, DateTime, Time

import dqlitedbapi.exceptions as dbapi_exc
from sqlalchemydqlite import DqliteDialect

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
