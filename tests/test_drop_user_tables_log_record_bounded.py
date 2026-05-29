"""Pin: _safe_for_log truncates server-supplied identifiers so an adversarial
sqlite_master table name cannot allocate an unbounded log record."""

from __future__ import annotations

from sqlalchemydqlite.provision import _LOG_TRUNCATE_MAX_CHARS, _safe_for_log


def test_safe_for_log_truncates_large_input() -> None:
    huge = "x" * (1024 * 1024)  # 1 MiB
    result = _safe_for_log(huge)
    # Generous 4 KiB bound shows truncation fired without coupling to the exact cap.
    assert len(result) < 4096, (
        f"safe_for_log must truncate large server-supplied input; got "
        f"len={len(result)} for 1 MiB input"
    )
    assert "truncated" in result


def test_safe_for_log_preserves_small_input() -> None:
    small = "my_table"
    assert _safe_for_log(small) == small


def test_safe_for_log_sanitises_control_chars() -> None:
    # U+2028 LINE SEPARATOR: repr() doesn't escape it and journald treats it as a
    # record separator; the sanitiser strips it.
    sanitised = _safe_for_log("foo bar")
    assert " " not in sanitised


def test_safe_for_log_cap_is_documented_constant() -> None:
    assert _LOG_TRUNCATE_MAX_CHARS == 200
