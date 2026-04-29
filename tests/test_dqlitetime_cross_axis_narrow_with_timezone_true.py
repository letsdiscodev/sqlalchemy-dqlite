"""Pin: ``_DqliteTime`` cross-axis narrow with ``timezone=True``.

Two earlier fixes interlock at one code path inside
``_DqliteTime.result_processor``:

- ``_DqliteTime`` narrows ``datetime.datetime`` to ``time`` via
  ``.time()`` (always drops tzinfo, mirroring pysqlite).
- When ``Time(timezone=True)`` and the resulting ``time`` is naive,
  UTC is re-attached via ``.replace(tzinfo=datetime.UTC)``.

The fall-through case — a ``datetime.datetime`` (aware or naive)
arriving at a ``Time(timezone=True)`` column → narrowed to naive
``time`` → re-attached UTC — is exercised IMPLICITLY by other tests
but never pinned directly. A future refactor that splits the two
branches (or moves the ``.time()`` narrow into the explicit datetime
branch only and skips the fall-through into the ``time`` branch)
could silently break the implicit fall-through.
"""

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

    def test_aware_datetime_narrows_loses_tz_then_re_attaches_utc(self) -> None:
        # Aware datetime in a non-UTC offset; ``.time()`` drops tzinfo,
        # then the naive-time → UTC re-attach branch fires.
        plus5 = datetime.timezone(datetime.timedelta(hours=5))
        aware_dt = datetime.datetime(2024, 1, 2, 12, 30, 45, tzinfo=plus5)
        proc = _DqliteTime(timezone=True).result_processor(None, None)
        assert proc is not None
        result = proc(aware_dt)
        # The wall-clock components are preserved verbatim — there is
        # no date dimension to anchor a tz conversion against, so this
        # mirrors pysqlite's "drop and re-attach" semantics rather
        # than performing a UTC offset shift.
        assert result == datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert isinstance(result, datetime.time)
        assert not isinstance(result, datetime.datetime)
