"""``_DqliteDateTime.bind_processor`` widens ``datetime.date`` → midnight
``datetime`` before handing the value to the driver. Mirrors pysqlite's
``DATETIME.bind_processor`` so cross-writer parity is preserved: a
sibling pysqlite writer stores the canonical full-timestamp shape
(``"2021-03-15 00:00:00.000000"``), not a date-only form.
"""

from __future__ import annotations

import datetime

from sqlalchemydqlite.base import _DqliteDateTime


def test_bind_date_widens_to_midnight_datetime_with_microseconds() -> None:
    """Pysqlite-parity: a bare ``datetime.date`` widens to the
    canonical full-timestamp shape ``"YYYY-MM-DD 00:00:00.000000"``
    (six trailing zeros in the fractional component) so cross-writer
    literal-string predicates round-trip bit-identically against
    sibling pysqlite writers. The bind_processor produces a string
    directly because ``_iso8601_from_datetime`` would otherwise omit
    the ``.000000`` when ``microsecond == 0``."""
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    widened = proc(datetime.date(2021, 3, 15))
    assert widened == "2021-03-15 00:00:00.000000"


def test_bind_datetime_formats_with_six_fractional_digits() -> None:
    """Naive ``datetime`` is formatted directly with six fractional
    digits so a ``microsecond == 0`` value still emits
    ``"...nn.000000"`` — pysqlite-parity. The previous identity
    pass-through routed through dqlitedbapi's encoder which omits
    the suffix when microseconds are zero, breaking cross-writer
    literal-string predicates."""
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    dt = datetime.datetime(2021, 3, 15, 12, 30, 45)
    assert proc(dt) == "2021-03-15 12:30:45.000000"


def test_bind_aware_datetime_formats_with_offset_suffix() -> None:
    """Tz-aware ``datetime`` is formatted with six fractional digits
    AND the ``±HH:MM`` offset suffix. The dbapi-layer
    ``_format_utc_offset`` helper renders the suffix so whole-minute
    vs sub-minute handling stays in lockstep with the wire codec."""
    proc = _DqliteDateTime(timezone=True).bind_processor(None)
    assert proc is not None
    tz = datetime.timezone(datetime.timedelta(hours=5))
    dt = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=tz)
    assert proc(dt) == "2021-03-15 12:30:45.000000+05:00"


def test_bind_none_passes_through() -> None:
    proc = _DqliteDateTime(timezone=False).bind_processor(None)
    assert proc is not None
    assert proc(None) is None
