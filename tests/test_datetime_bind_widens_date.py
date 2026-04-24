"""``_DqliteDateTime.bind_processor`` widens ``datetime.date`` → midnight
``datetime`` before handing the value to the driver. Mirrors pysqlite's
``DATETIME.bind_processor`` so cross-writer parity is preserved: a
sibling pysqlite writer stores the canonical full-timestamp shape
(``"2021-03-15 00:00:00.000000"``), not a date-only form.
"""

from __future__ import annotations

import datetime

from sqlalchemydqlite.base import _DqliteDateTime


def test_bind_date_widens_to_midnight_datetime() -> None:
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    widened = proc(datetime.date(2021, 3, 15))
    assert isinstance(widened, datetime.datetime)
    assert widened == datetime.datetime(2021, 3, 15, 0, 0, 0)


def test_bind_datetime_passes_through_unchanged() -> None:
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    dt = datetime.datetime(2021, 3, 15, 12, 30, 45)
    assert proc(dt) is dt


def test_bind_aware_datetime_preserves_tz() -> None:
    proc = _DqliteDateTime(timezone=True).bind_processor(None)
    assert proc is not None
    tz = datetime.timezone(datetime.timedelta(hours=5))
    dt = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=tz)
    assert proc(dt) is dt  # unchanged object identity


def test_bind_none_passes_through() -> None:
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    assert proc(None) is None
