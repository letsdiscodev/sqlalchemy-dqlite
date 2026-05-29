"""DEBUG arm on a malformed cell is gated by ``isEnabledFor(DEBUG)`` so the
per-row ``_safe_for_log`` walk does not run when DEBUG is filtered out."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

from sqlalchemy.dialects.sqlite import DATETIME, TIME

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


def _arm_warn_one_shot(cls: type) -> None:
    """Flip the one-shot WARN gate so the DEBUG arm is exercised."""
    cls._unparseable_iso_warning_emitted = True  # type: ignore[attr-defined]


def _make_processor_datetime() -> Any:
    inst = _DqliteDateTime()
    return inst.result_processor(None, DATETIME)


def _make_processor_date() -> Any:
    inst = _DqliteDate()
    return inst.result_processor(None, DATETIME)


def _make_processor_time() -> Any:
    inst = _DqliteTime()
    return inst.result_processor(None, TIME)


def _run_processor_n_times(processor: Any, n: int) -> None:
    for _ in range(n):
        processor("not-an-iso-string")


def test_datetime_debug_arm_does_not_call_sanitiser_when_debug_filtered() -> None:
    _arm_warn_one_shot(_DqliteDateTime)
    processor = _make_processor_datetime()

    sanitiser_calls = 0

    def counting_safe_for_log(value: Any) -> str:
        nonlocal sanitiser_calls
        sanitiser_calls += 1
        return str(value)

    with (
        patch("sqlalchemydqlite.base._safe_for_log", side_effect=counting_safe_for_log),
        patch.object(logging.getLogger("sqlalchemydqlite.base"), "level", logging.WARNING),
    ):
        sa_logger = logging.getLogger("sqlalchemydqlite.base")
        previous = sa_logger.level
        sa_logger.setLevel(logging.WARNING)
        try:
            _run_processor_n_times(processor, 100)
        finally:
            sa_logger.setLevel(previous)

    assert sanitiser_calls == 0, (
        f"_safe_for_log was called {sanitiser_calls} times under WARN-only "
        "logger config; expected zero (DEBUG arm should be skipped via "
        "isEnabledFor gate)"
    )


def test_datetime_debug_arm_calls_sanitiser_when_debug_enabled() -> None:
    _arm_warn_one_shot(_DqliteDateTime)
    processor = _make_processor_datetime()

    sanitiser_calls = 0

    def counting_safe_for_log(value: Any) -> str:
        nonlocal sanitiser_calls
        sanitiser_calls += 1
        return str(value)

    sa_logger = logging.getLogger("sqlalchemydqlite.base")
    previous = sa_logger.level
    sa_logger.setLevel(logging.DEBUG)
    try:
        with patch("sqlalchemydqlite.base._safe_for_log", side_effect=counting_safe_for_log):
            _run_processor_n_times(processor, 10)
    finally:
        sa_logger.setLevel(previous)

    # Two _safe_for_log calls per row (value + str(e)).
    assert sanitiser_calls == 20, (
        f"expected 20 sanitiser calls (10 rows × 2 args), got {sanitiser_calls}"
    )


def test_date_debug_arm_does_not_call_sanitiser_when_debug_filtered() -> None:
    _arm_warn_one_shot(_DqliteDate)
    processor = _make_processor_date()

    sanitiser_calls = 0

    def counting_safe_for_log(value: Any) -> str:
        nonlocal sanitiser_calls
        sanitiser_calls += 1
        return str(value)

    sa_logger = logging.getLogger("sqlalchemydqlite.base")
    previous = sa_logger.level
    sa_logger.setLevel(logging.WARNING)
    try:
        with patch("sqlalchemydqlite.base._safe_for_log", side_effect=counting_safe_for_log):
            _run_processor_n_times(processor, 100)
    finally:
        sa_logger.setLevel(previous)

    assert sanitiser_calls == 0


def test_time_debug_arm_does_not_call_sanitiser_when_debug_filtered() -> None:
    _arm_warn_one_shot(_DqliteTime)
    processor = _make_processor_time()

    sanitiser_calls = 0

    def counting_safe_for_log(value: Any) -> str:
        nonlocal sanitiser_calls
        sanitiser_calls += 1
        return str(value)

    sa_logger = logging.getLogger("sqlalchemydqlite.base")
    previous = sa_logger.level
    sa_logger.setLevel(logging.WARNING)
    try:
        with patch("sqlalchemydqlite.base._safe_for_log", side_effect=counting_safe_for_log):
            for _ in range(100):
                processor("not-a-time-string")
    finally:
        sa_logger.setLevel(previous)

    assert sanitiser_calls == 0
