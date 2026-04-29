"""Pin: SA bind processors must reject cross-type payloads that the
result-side already rejects, so a bind→read round-trip via the same
dialect doesn't write a cell that the same dialect's reader rejects.

The cycle 18 result-side closures (ISSUE-804/805/805+) raise
``DataError`` on:
- ``DateTime`` column receiving ``datetime.time`` (no defensible date)
- ``Date`` column receiving ``datetime.time`` (no defensible date)
- ``Time`` column receiving ``datetime.datetime`` → narrow

Symmetric bind-side fixes:
- ``_DqliteDateTime.bind_processor``: reject ``datetime.time`` (raise)
- ``_DqliteDate.bind_processor``: define one (was None) — narrow
  ``datetime.datetime`` to ``.date()``, reject ``datetime.time``

Pysqlite parity reference: pysqlite's DATE.bind_processor emits only
``YYYY-MM-DD`` and rejects time inputs; pysqlite's DATETIME.bind_processor
likewise rejects time. Cycle-18 chose ``DataError`` (PEP 249) for the
result-side rejections; use the same class for bind-side symmetry.
"""

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
        proc = _DqliteDateTime(timezone=False).bind_processor(None)
        assert proc is not None
        dt = datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert proc(dt) == dt

    def test_bare_date_still_widens_to_midnight(self) -> None:
        """The pysqlite-parity widen at the existing bind-side
        branch must not regress."""
        proc = _DqliteDateTime(timezone=False).bind_processor(None)
        assert proc is not None
        d = datetime.date(2024, 1, 2)
        assert proc(d) == datetime.datetime(2024, 1, 2, 0, 0, 0)


class TestDqliteDateBindNarrowsDatetimeAndRejectsTime:
    def test_datetime_payload_narrows_to_date(self) -> None:
        """A ``datetime`` bound to a Date column must serialize as
        ``YYYY-MM-DD`` only — pysqlite-parity. Without the narrowing
        the dbapi writes a full timestamp into a Date cell, breaking
        cross-writer parity."""
        proc = _DqliteDate().bind_processor(None)
        assert proc is not None
        dt = datetime.datetime(2020, 5, 17, 14, 30, 0)
        result = proc(dt)
        assert result == datetime.date(2020, 5, 17)
        assert isinstance(result, datetime.date)
        # datetime.date is NOT a subclass of datetime.datetime
        # (the reverse is true), so the narrow is observable.
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
