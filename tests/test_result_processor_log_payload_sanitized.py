"""Date/Time/DateTime result_processor log payloads route through
``_safe_for_log`` so control/bidi/invisible chars (e.g. U+2028, read as a
journald record separator) cannot inject into log lines."""

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.base import (
    _DqliteDate,
    _DqliteDateTime,
    _DqliteTime,
    _safe_for_log,
)


@pytest.fixture
def _dialect() -> Any:
    return MagicMock()


def test_safe_for_log_strips_u2028_line_separator() -> None:
    """journald treats U+2028 as a record separator; it must be stripped."""
    payload = "before after"
    out = _safe_for_log(payload)
    assert " " not in out


def test_safe_for_log_strips_bidi_override() -> None:
    """U+202E (RIGHT-TO-LEFT OVERRIDE) is a classic log-injection vector."""
    payload = "before‮after"
    out = _safe_for_log(payload)
    assert "‮" not in out


def test_safe_for_log_strips_zero_width_space() -> None:
    payload = "before​after"
    out = _safe_for_log(payload)
    assert "​" not in out


def test_safe_for_log_truncates_long_payload() -> None:
    payload = "X" * 10_000
    out = _safe_for_log(payload)
    assert len(out) < 1024
    assert "truncated" in out


def test_datetime_processor_log_strips_u2028(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = _DqliteDateTime().result_processor(_dialect, None)
    assert proc is not None

    payload = "garbage ‮-injected"
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        proc(payload)

    rendered = "\n".join(r.message for r in caplog.records)
    assert " " not in rendered
    assert "‮" not in rendered


def test_date_processor_log_strips_u2028(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = _DqliteDate().result_processor(_dialect, None)
    assert proc is not None
    payload = "garbage "
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        proc(payload)
    rendered = "\n".join(r.message for r in caplog.records)
    assert " " not in rendered


def test_time_processor_log_strips_u2028(
    _dialect: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = _DqliteTime().result_processor(_dialect, None)
    assert proc is not None
    payload = "garbage "
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        proc(payload)
    rendered = "\n".join(r.message for r in caplog.records)
    assert " " not in rendered
