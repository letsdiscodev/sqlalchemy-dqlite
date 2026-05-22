"""Pin: ``_drop_user_tables`` truncates server-supplied identifiers
before logging so a pathological table name from ``sqlite_master``
cannot allocate an unbounded log record.

Threat model: multi-tenant cluster where adversarial table names can
be created. The wire-layer ``_MAX_TEXT_VALUE_SIZE`` (~64 MiB) does not
apply to ``sqlite_master`` row values (column-name cap is unrelated).
A megabyte-long table name would, without truncation, allocate a
megabyte-long DEBUG log record.

The provision-side ``_safe_for_log`` composes the wire-layer
``sanitize_for_log`` (sanitisation) with the project-wide 200-char
truncation cap, matching the discipline already in place in
``base.py``.
"""

from __future__ import annotations

from sqlalchemydqlite.provision import _LOG_TRUNCATE_MAX_CHARS, _safe_for_log


def test_safe_for_log_truncates_large_input() -> None:
    huge = "x" * (1024 * 1024)  # 1 MiB
    result = _safe_for_log(huge)
    # The sanitised+truncated form is bounded by the cap plus a small
    # overhead for the "... [truncated, N chars]" suffix; a generous
    # 4 KiB bound demonstrates the truncation fired without coupling
    # to the exact cap.
    assert len(result) < 4096, (
        f"safe_for_log must truncate large server-supplied input; got "
        f"len={len(result)} for 1 MiB input"
    )
    assert "truncated" in result


def test_safe_for_log_preserves_small_input() -> None:
    small = "my_table"
    assert _safe_for_log(small) == small


def test_safe_for_log_sanitises_control_chars() -> None:
    # U+2028 LINE SEPARATOR is the load-bearing case: repr() does not
    # escape it, journald treats it as a record separator. The
    # sanitiser strips it.
    sanitised = _safe_for_log("foo bar")
    assert " " not in sanitised


def test_safe_for_log_cap_is_documented_constant() -> None:
    # Sanity pin so the cap can't drift to a different value without
    # the test landing alongside.
    assert _LOG_TRUNCATE_MAX_CHARS == 200
