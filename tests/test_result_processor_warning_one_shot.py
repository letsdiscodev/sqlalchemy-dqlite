"""Pin: SA Date/Time/DateTime result_processor closures emit at most
one WARNING per processor instance for unparseable ISO8601 cells.

Without the one-shot gate, a SELECT against a corrupted column
emits one WARNING per row (up to ``max_total_rows``), drowning
operator log pipelines. The first row's WARNING is preserved as
the forensic anchor; subsequent rows demote to DEBUG.
"""

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


@pytest.fixture
def _dialect() -> Any:
    return MagicMock()


def _drive_n_bad_rows(
    proc: Any,
    n: int,
    payload: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        for _ in range(n):
            proc(payload)


def test_datetime_processor_emits_one_warning_for_many_bad_rows(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = _DqliteDateTime().result_processor(_dialect, None)
    assert proc is not None
    _drive_n_bad_rows(proc, 50, "not-iso-at-all", caplog)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    debugs = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert len(warnings) == 1, f"expected 1 WARNING, got {len(warnings)}"
    assert len(debugs) == 49


def test_date_processor_emits_one_warning_for_many_bad_rows(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = _DqliteDate().result_processor(_dialect, None)
    assert proc is not None
    _drive_n_bad_rows(proc, 50, "not-iso-at-all", caplog)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    debugs = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert len(warnings) == 1
    assert len(debugs) == 49


def test_time_processor_emits_one_warning_for_many_bad_rows(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = _DqliteTime().result_processor(_dialect, None)
    assert proc is not None
    _drive_n_bad_rows(proc, 50, "not-iso-at-all", caplog)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    debugs = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert len(warnings) == 1
    assert len(debugs) == 49


def test_one_shot_is_per_processor_instance_not_global(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Each result_processor call (per query) returns a fresh
    closure with its own gate — the WARNING fires once per query,
    not once per process."""
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        proc1 = _DqliteDateTime().result_processor(_dialect, None)
        proc2 = _DqliteDateTime().result_processor(_dialect, None)
        assert proc1 is not None and proc2 is not None
        proc1("bad-1")
        proc1("bad-2")
        proc2("bad-3")
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 2
