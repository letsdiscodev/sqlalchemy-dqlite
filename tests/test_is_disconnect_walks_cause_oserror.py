"""is_disconnect's cause-chain walk classifies OSError nodes deeper than
the bare exception (the bare-e check only covers the top-level case)."""

from __future__ import annotations

from sqlalchemydqlite.base import DqliteDialect


def test_oserror_at_root_classified() -> None:
    """Control: bare-e OSError still classified."""
    e = OSError("Connection reset by peer")
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_oserror_via_cause_chain_classified() -> None:
    """OSError sitting as __cause__ under a different wrapper class → True."""

    class _UserWrapper(Exception):
        pass

    try:
        try:
            raise OSError("ECONNRESET")
        except OSError as os_err:
            raise _UserWrapper("retry budget exhausted") from os_err
    except _UserWrapper as wrapped:
        assert DqliteDialect().is_disconnect(wrapped, None, None) is True


def test_oserror_via_context_chain_classified() -> None:
    """__context__ (no ``from``) is also walked."""

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
    """A wrapper with no OSError and no disconnect substring → False."""

    class _UserWrapper(Exception):
        pass

    e = _UserWrapper("something application-level")
    assert DqliteDialect().is_disconnect(e, None, None) is False
