"""SA result processors must not silently pass through cross-type
payloads.

``dqlitedbapi._datetime_from_iso8601`` is intentionally polymorphic —
a time-only ISO string parses to ``datetime.time``, a full datetime
string parses to ``datetime.datetime``. When such a value lands in a
column whose declared type promises a different concrete type, the
processor must surface the type-contract violation rather than
silently leak the wrong concrete type to the ORM consumer.

Two cases:

- ``_DqliteDateTime`` (DateTime column) receives ``datetime.time``:
  no sensible coercion exists (no date to fabricate); raise
  ``DataError``. Sibling-parity rationale: pysqlite raises on TEXT
  payloads it can't parse, and there is no narrowing analogue.

- ``_DqliteTime`` (Time column) receives ``datetime.datetime``:
  narrow via ``value.time()`` (drop the date component). Sibling
  parity with ``_DqliteDate`` which already narrows ``datetime ->
  date`` via ``value.date()`` (line 284 of base.py). Both are
  "cell carries more information than the column contract requires;
  drop the extra dimension."
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


class TestDqliteDateTimeRejectsTimeOnlyPayload:
    """``DateTime`` column receiving a ``datetime.time`` must raise
    rather than silently pass it through."""

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
        """The new guard must not perturb the well-formed path."""
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert proc(dt) == dt


class TestDqliteTimeNarrowsDatetimePayload:
    """``Time`` column receiving a ``datetime.datetime`` narrows via
    ``.time()`` — sibling parity with ``_DqliteDate`` narrowing
    ``datetime -> date`` via ``.date()``."""

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
        """``datetime.datetime.time()`` always drops tzinfo (the
        tz-preserving analogue is ``.timetz()``). The narrowing
        therefore yields a naive ``datetime.time``, matching
        ``_DqliteDate``'s "tzinfo is dropped" deliberate decision."""
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
    """``Date`` column receiving a ``datetime.time`` must raise —
    symmetric to ``_DqliteDateTime`` (no defensible date to
    fabricate). Without the fix, the time would silently leak to the
    ORM consumer."""

    def test_time_only_payload_raises_data_error(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        with pytest.raises(DataError, match="time-only|datetime.time"):
            proc(datetime.time(12, 30, 0))

    def test_well_formed_datetime_still_narrows_to_date(self) -> None:
        """Sanity: the existing datetime->date narrowing must not
        regress."""
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 12, 30, 0)
        assert proc(dt) == datetime.date(2024, 1, 2)

    def test_well_formed_date_still_passes(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        d = datetime.date(2024, 1, 2)
        assert proc(d) == d
