"""is_disconnect's narrow InterfaceError "closed" branch must descend the cause chain
so middleware-wrapped variants still classify as disconnect."""

from __future__ import annotations

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class TestInterfaceErrorClosedCauseWalk:
    def test_direct_connection_is_closed_still_matches(self) -> None:
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("Connection is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_direct_cursor_is_closed_still_matches(self) -> None:
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("Cursor is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_one_hop_wrapped_connection_is_closed_matches(self) -> None:
        """The walk must descend one hop through ``raise X from inner``."""
        dialect = DqliteDialect()
        inner = _dbapi_exc.InterfaceError("Connection is closed")
        try:
            raise _dbapi_exc.InterfaceError("wrapped middleware error") from inner
        except _dbapi_exc.InterfaceError as wrapped:
            # Only the wrapped cause has "closed"; a single-hop check would miss this.
            assert "closed" not in str(wrapped).lower().split("from")[0]
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_one_hop_wrapped_cursor_is_closed_matches(self) -> None:
        dialect = DqliteDialect()
        inner = _dbapi_exc.InterfaceError("Cursor is closed")
        try:
            raise _dbapi_exc.InterfaceError("wrapped middleware error") from inner
        except _dbapi_exc.InterfaceError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_two_hop_wrapped_closed_matches(self) -> None:
        """Two layers of wrap must still be walked."""
        dialect = DqliteDialect()
        inner = _dbapi_exc.InterfaceError("Connection is closed")
        try:
            try:
                raise inner
            except _dbapi_exc.InterfaceError as e:
                raise _dbapi_exc.InterfaceError("wrap 1") from e
        except _dbapi_exc.InterfaceError as wrap1:
            try:
                raise _dbapi_exc.InterfaceError("wrap 2") from wrap1
            except _dbapi_exc.InterfaceError as outer:
                assert dialect.is_disconnect(outer, None, None) is True

    def test_context_fallback_when_cause_is_none(self) -> None:
        """Implicit __context__ (raise inside except without from) must also be traversed."""
        dialect = DqliteDialect()
        try:
            try:
                raise _dbapi_exc.InterfaceError("Connection is closed")
            except _dbapi_exc.InterfaceError:
                raise _dbapi_exc.InterfaceError("implicit chain")  # noqa: B904
        except _dbapi_exc.InterfaceError as outer:
            assert dialect.is_disconnect(outer, None, None) is True

    def test_unrelated_interface_error_still_returns_false(self) -> None:
        """A non-closed InterfaceError MUST NOT classify; the substring set stays narrow."""
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("invalid argument shape")
        assert dialect.is_disconnect(e, None, None) is False

    def test_wrapped_unrelated_interface_error_still_returns_false(self) -> None:
        """Wrapping a non-closed InterfaceError must still return False."""
        dialect = DqliteDialect()
        inner = _dbapi_exc.InterfaceError("invalid argument shape")
        try:
            raise _dbapi_exc.InterfaceError("wrapped") from inner
        except _dbapi_exc.InterfaceError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is False
