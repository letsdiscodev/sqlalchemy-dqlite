"""is_disconnect's InterfaceError "closed" branch walks the cause chain.

The other two branches that classify a slot as "dead" walk the
``__cause__`` / ``__context__`` chain via ``_walk_cause_chain``:

- the ``DqliteConnectionError`` / ``ClusterError`` branch
- the leader-change ``OperationalError`` code branch

The narrow ``InterfaceError("connection is closed")`` /
``InterfaceError("cursor is closed")`` branch was originally a single
hop. A retry decorator, telemetry middleware, or circuit breaker that
wraps the InterfaceError in another exception silently dropped the
disconnect signal — the SA pool slot stayed alive while the underlying
dbapi handle was already closed, surfacing later as a confusing
"connection closed" on the next user statement.

Pin the symmetric walk: the InterfaceError "closed" check must descend
through the cause chain so wrapped variants are still classified as
disconnect.
"""

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
        """Middleware wraps the InterfaceError in another exception via
        ``raise X from inner``. The walk must descend one hop."""
        dialect = DqliteDialect()
        inner = _dbapi_exc.InterfaceError("Connection is closed")
        try:
            raise _dbapi_exc.InterfaceError("wrapped middleware error") from inner
        except _dbapi_exc.InterfaceError as wrapped:
            # Outer message has no "closed" substring; only the
            # wrapped-cause does. Single-hop check would miss this.
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
        """Telemetry shim + retry decorator → two layers of wrap."""
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
        """Implicit ``__context__`` (via ``raise`` inside ``except``
        without ``from``) must also be traversed."""
        dialect = DqliteDialect()
        try:
            try:
                raise _dbapi_exc.InterfaceError("Connection is closed")
            except _dbapi_exc.InterfaceError:
                raise _dbapi_exc.InterfaceError("implicit chain")  # noqa: B904
        except _dbapi_exc.InterfaceError as outer:
            assert dialect.is_disconnect(outer, None, None) is True

    def test_unrelated_interface_error_still_returns_false(self) -> None:
        """Programming-error InterfaceErrors (e.g. setinputsizes on a
        closed cursor with a different message) MUST NOT be classified
        as disconnect even with the chain walk — the substring set
        stays narrow to "connection is closed" / "cursor is closed"."""
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("invalid argument shape")
        assert dialect.is_disconnect(e, None, None) is False

    def test_wrapped_unrelated_interface_error_still_returns_false(self) -> None:
        """Wrapping a non-closed InterfaceError must still return False
        — the walk only matches on the narrow closed-substring set."""
        dialect = DqliteDialect()
        inner = _dbapi_exc.InterfaceError("invalid argument shape")
        try:
            raise _dbapi_exc.InterfaceError("wrapped") from inner
        except _dbapi_exc.InterfaceError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is False
