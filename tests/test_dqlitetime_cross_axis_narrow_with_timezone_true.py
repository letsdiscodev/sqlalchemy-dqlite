"""Pin: ``_DqliteTime`` cross-axis narrow with ``timezone=True``.

Two interlocking branches inside ``_DqliteTime.result_processor``:

- ``_DqliteTime`` narrows ``datetime.datetime`` to ``time`` via
  ``.timetz()`` so the source ``tzinfo`` survives the narrow.
- When ``Time(timezone=True)`` and the resulting ``time`` is naive,
  UTC is re-attached via ``.replace(tzinfo=datetime.UTC)``; when
  the resulting ``time`` is already aware, the source offset is
  preserved (no silent UTC rewrite).

The fall-through case is exercised by ``test_dqlite_time_result_
processor_preserves_offset.py`` for the aware-datetime path; this
file pins the naive-datetime path (the canonical
``Time(timezone=True)`` re-attach contract) so a future refactor
that breaks the implicit fall-through is caught.
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

    def test_aware_datetime_narrows_preserves_source_offset(self) -> None:
        """A non-UTC aware datetime narrows via ``.timetz()`` so the
        source offset survives — the naive-time → UTC re-attach
        branch fires ONLY when the narrowed value is naive. The
        previous implementation used ``.time()`` and silently rewrote
        a non-UTC offset to ``+00:00``."""
        plus5 = datetime.timezone(datetime.timedelta(hours=5))
        aware_dt = datetime.datetime(2024, 1, 2, 12, 30, 45, tzinfo=plus5)
        proc = _DqliteTime(timezone=True).result_processor(None, None)
        assert proc is not None
        result = proc(aware_dt)
        assert isinstance(result, datetime.time)
        assert not isinstance(result, datetime.datetime)
        # Source offset preserved — NOT silently rewritten to UTC.
        assert result.utcoffset() == datetime.timedelta(hours=5)
