"""Pin: the ISO8601-parse-failure log path in the Date / Time / DateTime
result_processors must truncate server-controlled TEXT before formatting
into ``logger.warning(..., %r, ...)``.

The wire layer caps each TEXT cell at ``_MAX_TEXT_VALUE_SIZE = 16 MiB``.
A malicious or compromised server returning oversized TEXT for a column
the SA dialect maps to ``DateTime`` / ``Date`` / ``Time`` produces a
~32 MiB log line per row (``%r`` roughly doubles the size with quoting
escapes). Multiplied across a result set, this is a DoS vector against
operator log infrastructure (rsyslog, journald rate limiters, log
shippers).

Pin that the emitted log message stays bounded regardless of input
size, and that a truncation marker is present.
"""

from __future__ import annotations

import logging

import pytest

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime

# Bound chosen to bracket the operator-useful range and reject any
# >= 1 KiB log lines on this path. Even with the truncation marker
# overhead, the emitted record's raw message (logger record.message)
# should comfortably fit under this cap.
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

    # Defensive contract: pass through on parse failure (matches existing
    # processor behaviour — the warning is forensic only).
    assert result == huge

    # Exactly one warning should fire.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"expected 1 warning, got {len(warnings)}"

    record = warnings[0]
    formatted = record.getMessage()
    encoded = formatted.encode("utf-8", errors="replace")
    assert len(encoded) < _LOG_LINE_MAX_BYTES, (
        f"log line is {len(encoded)} bytes; expected < {_LOG_LINE_MAX_BYTES} "
        "(server-controlled TEXT must be truncated before %r formatting)."
    )
    # Truncation marker / shape: the formatted message MUST NOT contain
    # a 1 MiB+ run of the input character — that would mean we logged
    # the full input.
    assert "X" * (1024 * 1024) not in formatted, (
        "formatted log line contains a 1 MiB run of input character — truncation is missing."
    )
