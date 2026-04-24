"""``sqltypes.Time`` is mapped to ``_DqliteTime`` in
``DqliteDialect.colspecs``. dqlitedbapi decodes ISO8601 time
payloads into native ``datetime.time`` instances; pysqlite's
upstream ``TIME.result_processor`` calls ``str_to_time`` on the raw
cell and would raise ``TypeError`` on a ``datetime.time`` argument.
The Dqlite-specific processor pins the right handling.
"""

from __future__ import annotations

import datetime

from sqlalchemy import types as sqltypes

from sqlalchemydqlite.base import DqliteDialect, _DqliteTime


def test_colspec_maps_time_to_dqlite_time() -> None:
    assert DqliteDialect.colspecs[sqltypes.Time] is _DqliteTime


def test_result_processor_passes_native_time_through() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    t = datetime.time(12, 5, 57, 105580)
    assert proc(t) is t


def test_result_processor_parses_iso8601_string() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    assert proc("12:05:57.105580") == datetime.time(12, 5, 57, 105580)


def test_result_processor_passes_through_none() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    assert proc(None) is None


def test_result_processor_passes_through_unparseable_string() -> None:
    proc = _DqliteTime().result_processor(None, None)
    assert proc is not None
    assert proc("not a time") == "not a time"
