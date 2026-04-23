"""AsyncAdaptedConnection exposes isolation_level for SA probes.

SA's aiosqlite reference adapter exposes ``isolation_level`` as a
read/write property backed by the underlying connection; SA diagnostics
and third-party middleware call ``getattr(dbapi_conn, "isolation_level",
None)`` on several code paths. Without a property the probes would see
``None`` and either log "isolation unknown" or silently bypass a pin.

dqlite cannot weaken isolation (every statement goes through Raft
consensus), so the property is read-only and returns the constant
``"SERIALIZABLE"`` that matches ``DqliteDialect.get_isolation_level``.
"""

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
        """SA diagnostics probe via ``getattr(conn, "isolation_level",
        None)``; the property must match that pattern without raising."""
        adapter = _adapter()
        assert getattr(adapter, "isolation_level", None) == "SERIALIZABLE"

    def test_read_only(self) -> None:
        """No setter is exposed: dqlite cannot weaken isolation and SA's
        engine flow already short-circuits ``set_isolation_level``."""
        import pytest

        adapter = _adapter()
        with pytest.raises(AttributeError):
            adapter.isolation_level = "READ UNCOMMITTED"  # type: ignore[misc]
