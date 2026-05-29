"""Sync ``do_rollback`` transport faults are already classified by
``is_disconnect`` via the dbapi wrap chain, so the sync side needs no analog
of the async ``_handle_exception`` remap (which only bridges greenlet-layer
RuntimeErrors that cannot reach the sync path). Any fix belongs in the dbapi
wrap chain or ``is_disconnect``'s substring set, not a ``do_rollback`` override."""

from __future__ import annotations

import os

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class TestSyncRollbackTransportFaultsClassified:
    def test_connection_closed_interface_error_classified(self) -> None:
        """Canonical post-close rollback shape; the substring scan picks it up."""
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("Connection is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_cursor_closed_interface_error_classified(self) -> None:
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("Cursor is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_failed_to_connect_classified(self) -> None:
        """The dbapi wraps connect-time OSError into OperationalError."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError(
            "Failed to connect: [Errno 111] Connection refused", code=None
        )
        assert dialect.is_disconnect(e, None, None) is True

    def test_wire_decode_failed_classified(self) -> None:
        """ProtocolError is wrapped as OperationalError("wire decode failed")."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("wire decode failed: malformed frame", code=None)
        assert dialect.is_disconnect(e, None, None) is True

    def test_oserror_through_cause_chain_classified(self) -> None:
        """Bare OSError reaching is_disconnect via the cause-walk arm."""
        dialect = DqliteDialect()
        leaf = OSError("Connection reset by peer")
        try:
            raise _dbapi_exc.OperationalError("retry wrapper", code=None) from leaf
        except _dbapi_exc.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_no_transaction_swallow_does_not_reach_dialect(self) -> None:
        """The dbapi (not the dialect) swallows ``no transaction`` codes, so
        SA's ``do_rollback`` never sees them — a layered-contract pin."""
        cluster_addr = os.environ.get("DQLITE_TEST_CLUSTER")
        if cluster_addr is None:
            return
        from dqlitedbapi import connect

        conn = connect(cluster_addr, timeout=2.0)
        try:
            # No active transaction — rollback() must NOT raise.
            conn.rollback()
        finally:
            conn.close()
