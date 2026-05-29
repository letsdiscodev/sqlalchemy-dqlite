"""Pin: ``AsyncAdaptedConnection.__init__`` signature mirrors SA's
reference connector shape ``(self, dbapi, connection)`` (so copy-pasted
SA construction doesn't ``TypeError``), while also accepting the legacy
single-positional shape ``(raw_conn)`` for backward compatibility.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_init_signature_sa_reference_order() -> None:
    """Parameters appear as ``(self, dbapi, connection)`` per SA reference."""
    sig = inspect.signature(AsyncAdaptedConnection.__init__)
    params = list(sig.parameters)
    assert params[:3] == ["self", "dbapi", "connection"], (
        f"AsyncAdaptedConnection.__init__ signature drifted from SA "
        f"reference shape (self, dbapi, connection): got {params}"
    )


def test_init_sa_reference_positional_construction_succeeds() -> None:
    """SA-reference positional construction ``(dbapi, raw)`` succeeds."""
    dbapi_module = MagicMock(name="dbapi_module")
    raw_conn = MagicMock(name="raw_conn")

    adapter = AsyncAdaptedConnection(dbapi_module, raw_conn)

    assert adapter.dbapi is dbapi_module
    assert adapter.driver_connection is raw_conn


def test_init_legacy_single_positional_still_works() -> None:
    """Legacy single-positional ``(raw_conn)`` still works; ``dbapi`` is
    ``None``."""
    raw_conn = MagicMock(name="raw_conn")
    adapter = AsyncAdaptedConnection(raw_conn)
    assert adapter.driver_connection is raw_conn
    assert adapter.dbapi is None
