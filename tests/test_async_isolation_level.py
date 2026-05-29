"""AsyncAdaptedConnection exposes a read-only ``isolation_level`` for SA
probes; dqlite cannot weaken isolation, so it's a constant ``SERIALIZABLE``."""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


class TestAsyncAdaptedConnectionIsolationLevel:
    def test_reports_serializable(self) -> None:
        assert _adapter().isolation_level == "SERIALIZABLE"

    def test_getattr_with_default_returns_serializable(self) -> None:
        adapter = _adapter()
        assert getattr(adapter, "isolation_level", None) == "SERIALIZABLE"

    def test_read_only(self) -> None:
        import pytest

        adapter = _adapter()
        with pytest.raises(AttributeError):
            adapter.isolation_level = "READ UNCOMMITTED"  # type: ignore[misc]
