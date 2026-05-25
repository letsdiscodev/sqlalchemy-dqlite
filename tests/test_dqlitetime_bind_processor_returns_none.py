"""Pin: ``_DqliteTime.bind_processor`` returns a callable that
formats ``datetime.time`` with always-on six fractional digits.

The previous implementation returned ``None`` (SA bind-side opt-out)
and relied on dqlitedbapi's ``_iso8601_from_time`` encoder. Two
related defects motivated the override:

* Cross-type confusion — a ``datetime.datetime`` or ``datetime.date``
  bound to a Time column passed silently through the wire, then
  surfaced as the wrong concrete type on read or was lossily
  narrowed by the result processor.
* Cross-writer parity — ``_iso8601_from_time`` omits the fractional
  suffix when ``microsecond == 0``; pysqlite's
  ``TIME._storage_format`` always emits six zeros, so cross-writer
  literal-string predicates against pysqlite-written cells diverged
  by seven characters.

The new bind_processor raises ``DataError`` on the cross-type cases
and formats ``datetime.time`` directly with the six-digit suffix.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteTime


def test_bind_processor_returns_callable_for_default_dialect() -> None:
    """The override returns an actual callable; ``None`` was the
    previous SA-opt-out and no longer applies."""
    dialect: Any = MagicMock()
    proc = _DqliteTime().bind_processor(dialect)
    assert callable(proc)


def test_bind_processor_returns_callable_for_timezone_true_variant() -> None:
    dialect: Any = MagicMock()
    proc = _DqliteTime(timezone=True).bind_processor(dialect)
    assert callable(proc)
