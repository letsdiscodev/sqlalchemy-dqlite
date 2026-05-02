"""Coverage-gap tests collated from a focused audit of reachable but
untested branches in sqlalchemy-dqlite.

Currently covers:
- ``AsyncAdaptedConnection._handle_exception``'s "event loop closed"
  substring wrap (existing tests cover this only at the close /
  terminate layer; this pins the wrap directly).
"""

from __future__ import annotations

import pytest


class TestHandleExceptionEventLoopClosedWrap:
    """``_handle_exception`` wraps a ``RuntimeError("Event loop is closed")``
    raised by the underlying loop in an ``OperationalError`` with the
    "event loop closed:" prefix so SA's is_disconnect classifier
    recognises it as a transport-class disconnect."""

    def test_handle_exception_wraps_event_loop_closed_runtimeerror(self) -> None:
        from unittest.mock import MagicMock

        from dqlitedbapi.exceptions import OperationalError as DqliteOperationalError
        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        # The adapter consults ``_dqlite_disconnect_messages`` via base.py;
        # our error message is "Event loop is closed" which matches.
        adapter._connection = MagicMock()

        original = RuntimeError("Event loop is closed")
        with pytest.raises(DqliteOperationalError) as excinfo:
            adapter._handle_exception(original)
        assert "event loop closed:" in str(excinfo.value)

    def test_handle_exception_passes_through_unrelated_runtimeerror(self) -> None:
        """A RuntimeError WITHOUT the "Event loop is closed" substring
        is NOT wrapped — passes through. Pins the substring guard so a
        future refactor cannot widen the wrap unintentionally."""
        from unittest.mock import MagicMock

        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapter._connection = MagicMock()

        original = RuntimeError("some other unrelated error")
        with pytest.raises(RuntimeError) as excinfo:
            adapter._handle_exception(original)
        # The original RuntimeError comes through unchanged.
        assert excinfo.value is original
