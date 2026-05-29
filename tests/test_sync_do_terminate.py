"""Pin: sync dialect has ``has_terminate=True`` + a ``do_terminate`` routing through
``force_close_transport`` (0.5s), not the inherited do_close fallback (10s, blocks shutdown).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


class TestHasTerminatePinned:
    def test_flag_is_true(self) -> None:
        assert DqliteDialect.has_terminate is True

    def test_flag_is_local_to_class(self) -> None:
        """Pin lives in the class dict so an upstream default flip can't revert it."""
        assert "has_terminate" in DqliteDialect.__dict__

    def test_do_terminate_is_local_override(self) -> None:
        """Override must be on the class, not inherited via MRO (default falls back to do_close)."""
        assert "do_terminate" in DqliteDialect.__dict__


class TestDoTerminateDelegation:
    def test_delegates_to_force_close_transport(self) -> None:
        conn = MagicMock()

        DqliteDialect().do_terminate(conn)

        conn.force_close_transport.assert_called_once_with()

    def test_does_not_call_close(self) -> None:
        """Must not route through close(): it awaits _close_async, which can block on a read."""
        conn = MagicMock()

        DqliteDialect().do_terminate(conn)

        conn.close.assert_not_called()

    def test_swallows_force_close_transport_exception(self) -> None:
        """has_terminate=True promises SA a non-raising path; a raising transport must not crash."""
        conn = MagicMock()
        conn.force_close_transport.side_effect = RuntimeError("transport already gone")

        DqliteDialect().do_terminate(conn)

        conn.force_close_transport.assert_called_once_with()
