"""Pin: ``_DqliteTime.result_processor`` preserves the source
``tzinfo`` when narrowing a ``datetime.datetime`` payload, via
``.timetz()`` (NOT ``.time()`` which silently drops tzinfo).

A wire payload that decodes as
``datetime(2021, 3, 15, 12, 30, 45, tzinfo=timezone(timedelta(hours=-7)))``
previously narrowed via ``.time()`` (returning a naive time), then
the ``Time(timezone=True)`` branch's "value.tzinfo is None"
re-attach unconditionally bound UTC. The application saw a tz-aware
time as the contract promised, but with the WRONG offset — a
7-hour silent rewrite of the instant.

``.timetz()`` preserves the source tzinfo so the next branch's
"value already aware" arm fires and the offset survives.
``Time(timezone=False)`` is unaffected: it strips tzinfo
unconditionally per its own contract.
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteTime


def _make_proc(*, timezone: bool) -> Any:
    return _DqliteTime(timezone=timezone).result_processor(MagicMock(), None)


def test_aware_datetime_payload_preserves_source_offset() -> None:
    """A non-UTC tz-aware datetime narrows via ``.timetz()`` and the
    source offset survives — no silent UTC rewrite."""
    proc = _make_proc(timezone=True)
    minus7 = datetime.timezone(datetime.timedelta(hours=-7))
    payload = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=minus7)
    result = proc(payload)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is not None
    # The offset survives — NOT silently rewritten to UTC.
    assert result.utcoffset() == datetime.timedelta(hours=-7), (
        f"expected -7h offset; got {result.utcoffset()!r}"
    )


def test_naive_datetime_payload_still_attaches_utc_for_timezone_true() -> None:
    """A naive datetime input (no source tzinfo) still gets UTC
    attached under ``Time(timezone=True)`` — the existing contract
    for cells written without a tz suffix."""
    proc = _make_proc(timezone=True)
    payload = datetime.datetime(2021, 3, 15, 12, 30, 45)
    result = proc(payload)
    assert isinstance(result, datetime.time)
    assert result.utcoffset() == datetime.timedelta(0)


def test_timezone_false_strips_aware_datetime_tzinfo() -> None:
    """``Time(timezone=False)`` contract: naive output regardless of
    source tzinfo."""
    proc = _make_proc(timezone=False)
    minus7 = datetime.timezone(datetime.timedelta(hours=-7))
    payload = datetime.datetime(2021, 3, 15, 12, 30, 45, tzinfo=minus7)
    result = proc(payload)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is None
