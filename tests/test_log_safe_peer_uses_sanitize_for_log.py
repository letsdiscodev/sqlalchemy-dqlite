"""Pin: ``_log_safe_peer`` routes through ``sanitize_for_log``, not
the bare display sanitiser ``sanitize_server_text``.

The helper is the SSOT for every peer-log interpolation in the SA
dialect. ``sanitize_server_text`` strips C0/C1 / U+2028/U+2029 /
bidi / ZWSP but DELIBERATELY leaves LF / tab intact for multi-line
display rendering. ``sanitize_for_log`` layers an additional
LF-escape (``\\n`` literal) and tab-escape (``\\t`` literal) over
that strip and is the canonical helper for ``logger.*`` call sites
that interpolate server-derived text into a single log record.

Routing the helper through ``sanitize_server_text`` re-introduced
the CWE-117 vector the helper was added to close: a server-supplied
peer address carrying ``\\n`` survived the helper unchanged and
split the log record. This pin asserts the helper escapes LF / tab
as the two-byte literal sequences sanitize_for_log produces.
"""

from __future__ import annotations

from sqlalchemydqlite.base import _log_safe_peer


class _Stub:
    def __init__(self, address: object) -> None:
        self.address = address


def test_log_safe_peer_escapes_lf_as_literal_backslash_n() -> None:
    """LF must escape to the two-byte sequence backslash+n so a
    journald / syslog consumer cannot interpret the LF as a record
    separator (the CWE-117 bypass the helper was meant to close)."""
    sanitized = _log_safe_peer(_Stub("evil\nFAKE"))
    assert sanitized is not None
    assert "\n" not in sanitized, f"raw LF leaked past _log_safe_peer (CWE-117): {sanitized!r}"
    assert "\\n" in sanitized, (
        f"sanitize_for_log should escape LF as backslash+n; got {sanitized!r}"
    )


def test_log_safe_peer_escapes_tab_as_literal_backslash_t() -> None:
    """Tab must also escape so a column-oriented log parser cannot be
    tricked by an embedded tab."""
    sanitized = _log_safe_peer(_Stub("host\tinjected"))
    assert sanitized is not None
    assert "\t" not in sanitized
    assert "\\t" in sanitized


def test_log_safe_peer_ascii_host_survives_byte_for_byte() -> None:
    """Positive: a well-formed ASCII host:port is unchanged."""
    assert _log_safe_peer(_Stub("host:9001")) == "host:9001"
