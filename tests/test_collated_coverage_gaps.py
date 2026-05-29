"""Coverage-gap tests for reachable-but-untested branches."""

from __future__ import annotations

import pytest


class TestHandleExceptionEventLoopClosedWrap:
    """``_handle_exception`` wraps ``RuntimeError("Event loop is closed")``
    in an ``OperationalError`` prefixed "event loop closed:" so SA's
    is_disconnect classifier treats it as a transport disconnect."""

    def test_handle_exception_wraps_event_loop_closed_runtimeerror(self) -> None:
        from unittest.mock import MagicMock

        from dqlitedbapi.exceptions import OperationalError as DqliteOperationalError
        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapter._connection = MagicMock()

        original = RuntimeError("Event loop is closed")
        with pytest.raises(DqliteOperationalError) as excinfo:
            adapter._handle_exception(original)
        assert "event loop closed:" in str(excinfo.value)

    def test_handle_exception_passes_through_unrelated_runtimeerror(self) -> None:
        """A RuntimeError without the "Event loop is closed" substring
        passes through unwrapped (pins the substring guard)."""
        from unittest.mock import MagicMock

        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
        adapter._connection = MagicMock()

        original = RuntimeError("some other unrelated error")
        with pytest.raises(RuntimeError) as excinfo:
            adapter._handle_exception(original)
        assert excinfo.value is original
