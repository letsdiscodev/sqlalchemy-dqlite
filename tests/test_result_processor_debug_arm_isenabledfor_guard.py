"""Pin: the DateTime / Date / Time result-processor DEBUG arm
on a malformed cell is gated by ``logger.isEnabledFor(logging.DEBUG)``
so the per-row ``_safe_for_log`` sanitiser walk does not run when
the operator's level filters DEBUG out.

The WARN arm fires once per type class (gated by
``_unparseable_iso_warning_emitted``). The DEBUG arm previously
fired unconditionally for every subsequent malformed cell — even
when the handler was filtering DEBUG out. The sanitiser was
evaluated EAGERLY as a positional ``logger.debug`` argument, so
the cost was paid before the handler ever decided to drop the
record.

Production deployments typically run at WARNING level or higher.
On a corrupted-column 1M-row SELECT, the prior shape burned
~500 ms of pure-Python sanitiser CPU on the loop thread for log
lines the handler never emitted.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

from sqlalchemy.dialects.sqlite import DATETIME, TIME

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


def _arm_warn_one_shot(cls: type) -> None:
    """Flip the one-shot WARN gate so the DEBUG arm is the one
    exercised by the test."""
    cls._unparseable_iso_warning_emitted = True  # type: ignore[attr-defined]


def _make_processor_datetime() -> Any:
    """Build a result_processor that walks the malformed-cell path
    on plain string input."""
    inst = _DqliteDateTime()
    return inst.result_processor(None, DATETIME)


def _make_processor_date() -> Any:
    inst = _DqliteDate()
    return inst.result_processor(None, DATETIME)


def _make_processor_time() -> Any:
    inst = _DqliteTime()
    return inst.result_processor(None, TIME)


def _run_processor_n_times(processor: Any, n: int) -> None:
    """Drive the processor over N malformed values to exercise
    the DEBUG arm."""
    for _ in range(n):
        processor("not-an-iso-string")


def test_datetime_debug_arm_does_not_call_sanitiser_when_debug_filtered() -> None:
    """DEBUG-filtered logger: per-row sanitiser walks must NOT run
    on the DEBUG arm of the unparseable-ISO path."""
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
        # Force the dialect logger's effective level to WARNING.
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
    """DEBUG-enabled logger: sanitiser walks run as before."""
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
    # Avoid the time-only-fromisoformat path issuing a date error;
    # use a string that's clearly not parseable as time.
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
