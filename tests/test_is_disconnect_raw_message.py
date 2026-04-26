"""Pin: ``is_disconnect`` substring fallback reads ``raw_message`` first.

The client truncates display-strings at 1024 chars
(``_MAX_DISPLAY_MESSAGE``) — a server-emitted error message larger than
that loses any disconnect-shaped tail past the boundary. The dbapi
preserves the full text on ``raw_message`` precisely so downstream
classifiers (like the SA dialect's ``is_disconnect``) can still see it.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def test_disconnect_substring_past_truncation_boundary_classified_via_raw_message() -> None:
    """Construct an ``OperationalError`` whose truncated displayed
    message lacks the disconnect substring but whose ``raw_message``
    contains it — pin that ``is_disconnect`` reads ``raw_message`` and
    therefore returns True."""
    truncated = "constraint A failed; constraint B failed"
    full = truncated + " ... wire stream error: expected ROWS, got FAILURE"
    e = OperationalError(truncated, code=5, raw_message=full)
    # Sanity: `str(e)` must NOT contain the disconnect substring (we
    # rely on the truncated displayed form for the negative half of the
    # contract).
    assert "wire stream error" not in str(e).lower()
    assert "wire stream error" in (e.raw_message or "").lower()
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_substring_match_falls_back_to_str_when_raw_message_absent() -> None:
    """Negative pin: an ``OperationalError`` constructed without a
    ``raw_message`` must still match via ``str(cause)`` — the
    ``getattr(..., None) or str(cause)`` fallback is the wire that keeps
    the legacy path alive for OperationalErrors built without the
    raw-message channel (e.g., wire-decode errors at the dbapi wrap
    site that pass ``code=None`` and no ``raw_message``)."""
    e = OperationalError("Wire decode failed: corrupt frame")
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_empty_string_raw_message_falls_back_to_str_cause() -> None:
    """Pin: an empty-string ``raw_message`` triggers the ``or
    str(cause)`` fallback so the substring scan still hits the
    truncated displayed form. Without the falsy-OR (e.g., a
    ``raw_message if ... is not None else str(cause)`` rewrite), an
    empty raw_message would short-circuit the scan against ``""`` and
    drop the disconnect signal.

    The dbapi ``DatabaseError.__init__`` only auto-fills
    ``raw_message`` when it is passed as ``None``; an explicit
    ``raw_message=""`` is preserved verbatim. Internal call sites that
    construct OperationalErrors with an explicit empty ``raw_message``
    (placeholder while the full text is fetched out-of-band, defensive
    construction in middleware, etc.) must not lose disconnect
    classification.
    """
    e = OperationalError("Wire decode failed: corrupt frame", raw_message="")
    assert e.raw_message == ""
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_substring_branch_safe_for_non_dbapi_causes() -> None:
    """Negative pin: a non-dbapi exception (no ``raw_message``
    attribute) wrapped on the cause chain must not crash the classifier
    — the ``getattr(cause, "raw_message", None)`` lookup must return
    None and fall back to ``str(cause)``. We exercise via a plain
    ``OSError`` whose message contains a disconnect substring; the
    OSError-type branch (handled separately) returns True before the
    substring branch even fires, so this primarily pins
    "no AttributeError surfaces"."""
    # OSError is caught by the OSError type-check earlier in the chain
    # walk; this primarily proves the substring branch's getattr is
    # exception-safe.
    e = OSError("connection closed")
    DqliteDialect().is_disconnect(e, None, None)  # must not raise
