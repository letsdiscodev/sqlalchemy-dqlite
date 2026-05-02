"""Pin: ``is_disconnect`` cause-chain walk classifies OSError nodes
deeper than the bare exception as disconnect.

The bare-``e`` OSError check covers only the direct case (top-level
exception is an OSError). A user wrapper or middleware that catches
OSError and re-raises a non-OSError above it routes through the walk;
without a per-node OSError classification, the OSError sitting under
the wrapper layer is invisible — the substring scan only catches the
case where the wrapper put a transport-shaped substring in its
message.
"""

from __future__ import annotations

from sqlalchemydqlite.base import DqliteDialect


def test_oserror_at_root_classified() -> None:
    """Sanity / control: bare-``e`` OSError still classified."""
    e = OSError("Connection reset by peer")
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_oserror_via_cause_chain_classified() -> None:
    """A user wrapper that catches OSError and re-raises a different
    class — the OSError sits as ``__cause__``. Pin the walk classifies
    it (without this fix, the bare-``e`` check sees only the wrapper
    class and returns False)."""

    class _UserWrapper(Exception):
        """Stand-in for a user retry/middleware layer."""

    try:
        try:
            raise OSError("ECONNRESET")
        except OSError as os_err:
            raise _UserWrapper("retry budget exhausted") from os_err
    except _UserWrapper as wrapped:
        assert DqliteDialect().is_disconnect(wrapped, None, None) is True


def test_oserror_via_context_chain_classified() -> None:
    """``__context__`` (no ``from``) is also walked. Pin the same
    behaviour for an OSError sitting under an implicit-chained
    wrapper."""

    class _UserWrapper(Exception):
        pass

    try:
        try:
            raise OSError("ETIMEDOUT")
        except OSError:
            raise _UserWrapper("middleware error") from None  # noqa: B904
    except _UserWrapper as wrapped:
        # Re-attach implicit context so the walk sees it.
        os_err = OSError("ETIMEDOUT")
        wrapped.__context__ = os_err
        assert DqliteDialect().is_disconnect(wrapped, None, None) is True


def test_non_oserror_with_no_disconnect_substring_not_classified() -> None:
    """Negative pin: a wrapper exception with no OSError or
    disconnect-substring must NOT be classified as disconnect."""

    class _UserWrapper(Exception):
        pass

    e = _UserWrapper("something application-level")
    assert DqliteDialect().is_disconnect(e, None, None) is False
