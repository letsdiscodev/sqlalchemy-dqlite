"""Pin: ``_log_safe_peer`` uses ``sanitize_for_log`` (which escapes LF/tab),
not ``sanitize_server_text`` (which leaves LF/tab intact) — else CWE-117."""

from __future__ import annotations

from sqlalchemydqlite.base import _log_safe_peer


class _Stub:
    def __init__(self, address: object) -> None:
        self.address = address


def test_log_safe_peer_escapes_lf_as_literal_backslash_n() -> None:
    """LF must escape to backslash+n so a syslog consumer can't read it as a
    record separator (the CWE-117 bypass the helper closes)."""
    sanitized = _log_safe_peer(_Stub("evil\nFAKE"))
    assert sanitized is not None
    assert "\n" not in sanitized, f"raw LF leaked past _log_safe_peer (CWE-117): {sanitized!r}"
    assert "\\n" in sanitized, (
        f"sanitize_for_log should escape LF as backslash+n; got {sanitized!r}"
    )


def test_log_safe_peer_escapes_tab_as_literal_backslash_t() -> None:
    sanitized = _log_safe_peer(_Stub("host\tinjected"))
    assert sanitized is not None
    assert "\t" not in sanitized
    assert "\\t" in sanitized


def test_log_safe_peer_ascii_host_survives_byte_for_byte() -> None:
    assert _log_safe_peer(_Stub("host:9001")) == "host:9001"
