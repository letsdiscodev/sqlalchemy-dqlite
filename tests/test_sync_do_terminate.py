"""Pin: sync dialect carries ``has_terminate = True`` and a
:meth:`do_terminate` override that routes through the dbapi's
:meth:`Connection.force_close_transport`.

SA's pool gates its forced-disposal path on ``has_terminate``. The
inherited default is ``False`` (do_terminate falls back to do_close),
which would route ``engine.dispose()``'s forced reclaim through the
dbapi's bounded-by-``self._timeout`` close path (default 10 s). Under
partition + SIGTERM that 10 s blocks operator shutdown SLAs.

The local pin lands SA's terminate path on the bounded
``force_close_transport`` (gated on ``close_timeout``, default 0.5 s)
mirroring the async sibling's contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


class TestHasTerminatePinned:
    def test_flag_is_true(self) -> None:
        assert DqliteDialect.has_terminate is True

    def test_flag_is_local_to_class(self) -> None:
        """The pin must live in ``DqliteDialect.__dict__`` so an upstream
        ``DefaultDialect`` default flip cannot silently revert it."""
        assert "has_terminate" in DqliteDialect.__dict__

    def test_do_terminate_is_local_override(self) -> None:
        """The override must live on the sync dialect's own class, not be
        picked up via MRO from ``DefaultDialect`` (whose default
        ``do_terminate`` falls back to ``do_close``)."""
        assert "do_terminate" in DqliteDialect.__dict__


class TestDoTerminateDelegation:
    def test_delegates_to_force_close_transport(self) -> None:
        """``do_terminate`` calls ``dbapi_connection.force_close_transport()``."""
        conn = MagicMock()

        DqliteDialect().do_terminate(conn)

        conn.force_close_transport.assert_called_once_with()

    def test_does_not_call_close(self) -> None:
        """Forced reclaim must NOT route through ``close()`` — that path
        awaits ``_close_async`` which can block on a parked wire read."""
        conn = MagicMock()

        DqliteDialect().do_terminate(conn)

        conn.close.assert_not_called()

    def test_swallows_force_close_transport_exception(self) -> None:
        """``has_terminate=True`` promises SA a non-raising path. A
        partial-state connection whose ``force_close_transport`` raises
        must not crash the pool finalize."""
        conn = MagicMock()
        conn.force_close_transport.side_effect = RuntimeError("transport already gone")

        # Must not propagate.
        DqliteDialect().do_terminate(conn)

        conn.force_close_transport.assert_called_once_with()
