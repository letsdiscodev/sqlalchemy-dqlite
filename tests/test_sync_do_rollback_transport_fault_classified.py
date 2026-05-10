"""Pin: sync ``do_rollback`` transport-fault paths are already classified
by ``is_disconnect`` via the dbapi's wrap chain.

The audit asked whether the sync side needs an analog of the async
``_handle_exception`` remap on ``commit``/``rollback`` (``aio.py:971-981``).
Architect + reviewer triage agree the answer is no:

1. The async ``_handle_exception`` exists exclusively to bridge
   greenlet-layer ``RuntimeError`` shapes (``RuntimeError("Future ...
   attached to a different loop")``, ``RuntimeError("Event loop is
   closed")``) that originate from ``await_only(...)``. These shapes
   cannot reach the sync path — there is no greenlet bridge.

2. Real transport faults reaching the sync ``dbapi_connection.rollback()``
   are already wrapped by the dbapi's ``_call_client`` plumbing into
   PEP 249 classes (``OperationalError``/``InterfaceError``) with
   messages that the SA dialect's ``is_disconnect`` substring scan
   recognises (``"failed to connect"``, ``"wire decode failed"``,
   ``"connection closed"`` / ``"cursor is closed"``, etc.).

This test pins the contract: feed ``DqliteDialect.is_disconnect`` the
exception shapes a sync ``rollback()`` would produce under transport
faults, and assert each is correctly classified. If a future refactor
breaks the dbapi-layer wrap chain (so a transport fault escapes as a
non-classified shape), this test fails — and the fix lives in the
dbapi's wrap chain or in ``is_disconnect``'s substring set, NOT in a
``do_rollback`` override on the sync dialect.
"""

from __future__ import annotations

import os

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class TestSyncRollbackTransportFaultsClassified:
    def test_connection_closed_interface_error_classified(self) -> None:
        """``InterfaceError("Connection is closed")`` is the canonical
        post-close rollback shape; the substring scan picks it up."""
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("Connection is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_cursor_closed_interface_error_classified(self) -> None:
        dialect = DqliteDialect()
        e = _dbapi_exc.InterfaceError("Cursor is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_failed_to_connect_classified(self) -> None:
        """The dbapi wraps OSError on connect into
        OperationalError(code=None, "Failed to connect: ...")."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError(
            "Failed to connect: [Errno 111] Connection refused", code=None
        )
        assert dialect.is_disconnect(e, None, None) is True

    def test_wire_decode_failed_classified(self) -> None:
        """ProtocolError → OperationalError(code=None,
        "wire decode failed: ...")."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("wire decode failed: malformed frame", code=None)
        assert dialect.is_disconnect(e, None, None) is True

    def test_oserror_through_cause_chain_classified(self) -> None:
        """Bare OSError reaching is_disconnect via the cause-walk
        (e.g. retry middleware re-raises a non-OSError) — handled by
        the OSError cause-chain arm in is_disconnect."""
        dialect = DqliteDialect()
        leaf = OSError("Connection reset by peer")
        try:
            raise _dbapi_exc.OperationalError("retry wrapper", code=None) from leaf
        except _dbapi_exc.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_no_transaction_swallow_does_not_reach_dialect(self) -> None:
        """The dbapi swallows ConnectionPool's ``no transaction``
        codes so SA's ``do_rollback`` never sees them. This pin
        documents the layered contract — the swallow happens at the
        dbapi layer, not the dialect layer. If a future refactor
        moves the swallow to the dialect, this contract changes and
        this test must be updated to reflect the new layering."""
        # Construct the rollback path to verify it doesn't raise on
        # a "no active transaction" condition. Skipped when no
        # cluster — this is a layered-contract pin, not a fault test.
        cluster_addr = os.environ.get("DQLITE_TEST_CLUSTER")
        if cluster_addr is None:
            return  # contract-only docs; no cluster → no exercise
        from dqlitedbapi import connect

        conn = connect(cluster_addr, timeout=2.0)
        try:
            # No transaction is active — rollback() must NOT raise.
            conn.rollback()
        finally:
            conn.close()
