"""Pin: ``_log_safe_peer`` routes every interpolation of a peer
address through the wire-layer ``sanitize_server_text`` discipline
so CWE-117 (log injection) is mitigated at the log site itself, not
only at the construction-time ``parse_address`` gate.

The dqliteclient layer already wraps every operator-supplied address
in ``sanitize_for_log`` at every ``logger.*`` call site (see
``connection.py::_log_safe_address`` and the CWE-117-annotated
wraps in ``cluster.py`` / ``pool.py``). The SA layer was added later
and historically interpolated ``getattr(<conn>, "address", None)``
directly into ``peer=%s`` records. ``_log_safe_peer`` is the single
helper through which all 13 SA-layer peer-log sites now route, so a
``dial_func`` override that bypasses ``parse_address`` (or a future
refactor that updates ``_address`` post-redirect to report the
connected leader) cannot inject control characters into syslog /
journald records.
"""

from __future__ import annotations

from sqlalchemydqlite.base import _log_safe_peer


class _Stub:
    def __init__(self, address: object) -> None:
        self.address = address


def test_log_safe_peer_returns_none_when_address_attribute_missing() -> None:
    """Object without ``address`` attribute → ``None`` (matches
    ``getattr(..., 'address', None)``)."""
    assert _log_safe_peer(object()) is None


def test_log_safe_peer_returns_none_when_address_is_none() -> None:
    """``address=None`` (the pre-connect state) renders as
    ``None`` rather than the string ``"None"``."""
    assert _log_safe_peer(_Stub(None)) is None


def test_log_safe_peer_passes_clean_address_through() -> None:
    """A well-formed address passes through unchanged so operators
    see the actual peer in their logs."""
    assert _log_safe_peer(_Stub("10.0.0.1:9001")) == "10.0.0.1:9001"


def test_log_safe_peer_strips_crlf_injection_attempt() -> None:
    """The load-bearing defense: a server-supplied / bypass-the-gate
    address carrying CRLF must NOT split the log record. The wire-
    layer ``sanitize_server_text`` replaces CR with ``?`` (LF passes
    through to be escaped by the log handler / format)."""
    sanitized = _log_safe_peer(_Stub("evil\r\n[CRITICAL] forged\r\n10.0.0.1:9001"))
    assert sanitized is not None
    assert "\r" not in sanitized, "CR must be neutralized (CWE-117)"


def test_log_safe_peer_strips_journald_record_separator() -> None:
    """U+2028 (LINE SEPARATOR) splits journald records. Verify
    sanitization strips it."""
    sanitized = _log_safe_peer(_Stub("10.0.0.1:9001 forged"))
    assert sanitized is not None
    assert " " not in sanitized


def test_log_safe_peer_strips_bidi_override() -> None:
    """RTL override / bidi-block characters can visually misrepresent
    an address in a log viewer. Sanitization strips them."""
    sanitized = _log_safe_peer(_Stub("10.0.0.1:9001‮txt.evil:9001"))
    assert sanitized is not None
    assert "‮" not in sanitized
