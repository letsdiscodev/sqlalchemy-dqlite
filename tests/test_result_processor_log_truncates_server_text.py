"""ISO8601-parse-failure log path in the Date/Time/DateTime processors must
truncate server-controlled TEXT before ``%r`` formatting, else an oversized
TEXT cell (up to the wire's 16 MiB cap) is a per-row log-infra DoS vector."""

from __future__ import annotations

import logging

import pytest

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime

_LOG_LINE_MAX_BYTES = 4096


@pytest.mark.parametrize(
    "processor_factory",
    [
        lambda: _DqliteDateTime(timezone=False).result_processor(None, None),
        lambda: _DqliteDate().result_processor(None, None),
        lambda: _DqliteTime().result_processor(None, None),
    ],
    ids=["DateTime", "Date", "Time"],
)
def test_unparseable_iso8601_log_truncates_oversized_server_text(
    processor_factory: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    proc = processor_factory()  # type: ignore[operator]
    assert proc is not None

    huge = "X" * (16 * 1024 * 1024)  # 16 MiB — wire-layer max for TEXT.

    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        result = proc(huge)

    # Pass through on parse failure; the warning is forensic only.
    assert result == huge

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"expected 1 warning, got {len(warnings)}"

    record = warnings[0]
    formatted = record.getMessage()
    encoded = formatted.encode("utf-8", errors="replace")
    assert len(encoded) < _LOG_LINE_MAX_BYTES, (
        f"log line is {len(encoded)} bytes; expected < {_LOG_LINE_MAX_BYTES} "
        "(server-controlled TEXT must be truncated before %r formatting)."
    )
    # A 1 MiB+ run of the input char would mean we logged the full input.
    assert "X" * (1024 * 1024) not in formatted, (
        "formatted log line contains a 1 MiB run of input character — truncation is missing."
    )
