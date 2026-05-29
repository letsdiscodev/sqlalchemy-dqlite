"""Pin: ``_DqliteTime.result_processor`` narrows a datetime payload via ``.timetz()`` (NOT
``.time()``, which drops tzinfo and lets the timezone=True branch silently rebind UTC,
rewriting the offset)."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteTime


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
