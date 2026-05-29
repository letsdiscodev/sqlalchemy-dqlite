"""Date/Time/DateTime result_processors emit at most one WARNING per process
per type-class for unparseable ISO8601 cells (subsequent rows demote to DEBUG).
The gate is a ClassVar so processor churn does not re-arm it."""

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


def test_one_shot_gate_is_per_class_across_processor_instances(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The ClassVar gate fires once across freshly-built processor instances."""
    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        proc1 = _DqliteDateTime().result_processor(_dialect, None)
        proc2 = _DqliteDateTime().result_processor(_dialect, None)
        assert proc1 is not None and proc2 is not None
        proc1("bad-1")
        proc1("bad-2")
        proc2("bad-3")
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1, (
        f"per-class ClassVar gate must fire ONCE across processor "
        f"instances; got {len(warnings)} WARNING records"
    )


def test_one_shot_gate_is_per_type_class_not_global(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Each type class has its own ClassVar so per-type diagnostics surface."""
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        dt_proc = _DqliteDateTime().result_processor(_dialect, None)
        d_proc = _DqliteDate().result_processor(_dialect, None)
        t_proc = _DqliteTime().result_processor(_dialect, None)
        assert dt_proc is not None and d_proc is not None and t_proc is not None
        dt_proc("bad-datetime")
        d_proc("bad-date")
        t_proc("bad-time")
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 3, (
        f"per-class gates must each fire once; got {len(warnings)} WARNING records"
    )
