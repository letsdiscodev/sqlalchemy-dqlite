"""_log_safe_peer routes peer addresses through sanitize_server_text so a
bypass of the parse_address gate cannot inject control characters into
log records (CWE-117)."""

from __future__ import annotations

from sqlalchemydqlite.base import _log_safe_peer


class _Stub:
    def __init__(self, address: object) -> None:
        self.address = address


def test_log_safe_peer_returns_none_when_address_attribute_missing() -> None:
    """Object without an ``address`` attribute → None."""
    assert _log_safe_peer(object()) is None


def test_log_safe_peer_returns_none_when_address_is_none() -> None:
    """address=None renders as None, not the string "None"."""
    assert _log_safe_peer(_Stub(None)) is None


def test_log_safe_peer_passes_clean_address_through() -> None:
    """A well-formed address passes through unchanged."""
    assert _log_safe_peer(_Stub("10.0.0.1:9001")) == "10.0.0.1:9001"


def test_log_safe_peer_strips_crlf_injection_attempt() -> None:
    """CRLF in an address must not split the log record; CR is replaced."""
    sanitized = _log_safe_peer(_Stub("evil\r\n[CRITICAL] forged\r\n10.0.0.1:9001"))
    assert sanitized is not None
    assert "\r" not in sanitized, "CR must be neutralized (CWE-117)"


def test_log_safe_peer_strips_journald_record_separator() -> None:
    """U+2028 (LINE SEPARATOR) splits journald records; must be stripped."""
    sanitized = _log_safe_peer(_Stub("10.0.0.1:9001 forged"))
    assert sanitized is not None
    assert " " not in sanitized


def test_log_safe_peer_strips_bidi_override() -> None:
    """Bidi-override chars can misrepresent an address in a log viewer."""
    sanitized = _log_safe_peer(_Stub("10.0.0.1:9001‮txt.evil:9001"))
    assert sanitized is not None
    assert "‮" not in sanitized
