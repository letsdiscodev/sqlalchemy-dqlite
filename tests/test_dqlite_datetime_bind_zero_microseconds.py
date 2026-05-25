"""Pin: ``_DqliteDateTime.bind_processor`` emits exactly six
fractional digits for naive AND aware ``datetime`` values whose
``microsecond == 0``, mirroring the widen-branch's already-shipped
discipline.

The widen-date branch was previously patched to emit a literal
``" 00:00:00.000000"`` suffix because dqlitedbapi's
``_iso8601_from_datetime`` encoder omits the fractional component
when ``microsecond == 0`` and pysqlite's ``_storage_format`` always
emits six zeros (a seven-character divergence per bind). The same
asymmetry survived on the non-widen path — a plain
``datetime(2021, 3, 15, 12, 5, 57)`` fell through ``return value``
into the encoder and shipped ``"2021-03-15 12:05:57"`` while pysqlite
shipped ``"2021-03-15 12:05:57.000000"``. Cross-writer literal-string
predicates of the form ``WHERE col = '2021-03-15 12:05:57.000000'``
silently returned zero rows.
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteDateTime


def _bind() -> Any:
    return _DqliteDateTime().bind_processor(MagicMock())


def test_naive_datetime_zero_microseconds_emits_six_zeros() -> None:
    out = _bind()(datetime.datetime(2021, 3, 15, 12, 5, 57))
    assert out == "2021-03-15 12:05:57.000000", out


def test_naive_datetime_nonzero_microseconds_unchanged() -> None:
    """Existing behaviour: ``microsecond != 0`` still gets the
    six-digit suffix via the encoder."""
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, 123456)
    out = _bind()(value)
    assert out == "2021-03-15 12:05:57.123456", out


def test_widen_branch_date_unchanged() -> None:
    """The already-shipped widen-branch parity stays at
    ``"2021-03-15 00:00:00.000000"``."""
    out = _bind()(datetime.date(2021, 3, 15))
    assert out == "2021-03-15 00:00:00.000000", out


def test_aware_datetime_zero_microseconds_carries_offset() -> None:
    """Tz-aware values keep the offset suffix and gain the six
    fractional zeros."""
    tz = datetime.timezone(datetime.timedelta(hours=2))
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, tzinfo=tz)
    out = _bind()(value)
    assert out == "2021-03-15 12:05:57.000000+02:00", out


def test_aware_datetime_nonzero_microseconds_carries_offset() -> None:
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    value = datetime.datetime(2021, 3, 15, 12, 5, 57, 123456, tzinfo=tz)
    out = _bind()(value)
    assert out == "2021-03-15 12:05:57.123456-05:00", out


def test_none_passes_through() -> None:
    assert _bind()(None) is None
