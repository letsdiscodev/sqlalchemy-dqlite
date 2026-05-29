"""is_disconnect substring fallback reads raw_message first.

The client truncates display strings at 1024 chars; the dbapi keeps the
full text on raw_message so classifiers see a disconnect tail past it.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def test_disconnect_substring_past_truncation_boundary_classified_via_raw_message() -> None:
    """Substring absent from truncated str but present in raw_message →
    True. code=None so the OperationalError arm of the walk applies."""
    truncated = "constraint A failed; constraint B failed"
    full = truncated + " ... wire decode failed: corrupt frame"
    e = OperationalError(truncated, code=None, raw_message=full)
    assert "wire decode failed" not in str(e).lower()
    assert "wire decode failed" in (e.raw_message or "").lower()
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_substring_match_falls_back_to_str_when_raw_message_absent() -> None:
    """Without raw_message, match must still fall back to str(cause)."""
    e = OperationalError("Wire decode failed: corrupt frame")
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_empty_string_raw_message_falls_back_to_str_cause() -> None:
    """Empty-string raw_message must hit the ``or str(cause)`` fallback,
    not short-circuit the substring scan against ``""``."""
    e = OperationalError("Wire decode failed: corrupt frame", raw_message="")
    assert e.raw_message == ""
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_substring_branch_safe_for_non_dbapi_causes() -> None:
    """A non-dbapi cause (no raw_message attr) must not crash the
    classifier; the substring branch's getattr must be exception-safe."""
    e = OSError("connection closed")
    DqliteDialect().is_disconnect(e, None, None)  # must not raise
