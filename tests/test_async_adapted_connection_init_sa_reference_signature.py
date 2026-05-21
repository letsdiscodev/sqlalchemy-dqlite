"""Pin: ``AsyncAdaptedConnection.__init__`` signature mirrors SA's
reference connector shape ``(self, dbapi, connection)``.

SA reference 1: ``sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection``
SA reference 2: ``sqlalchemy.dialects.sqlite.aiosqlite.AsyncAdapt_aiosqlite_connection``

Both reference constructors place ``dbapi`` first as a positional
parameter; third-party instrumentation (Sentry, OpenTelemetry, Datadog,
sqlalchemy-utils) and SA's own ``AsyncAdaptFallback_*`` subclasses
construct adapters by mimicking this reference shape. A divergent
positional order surfaces ``TypeError`` on copy-pasted construction.

The constructor also accepts the legacy single-positional shape
(``AsyncAdaptedConnection(raw_conn)``) for backward compatibility with
existing test fixtures and any external code; runtime detection chooses
the right path based on whether the second positional argument was
supplied.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_init_signature_sa_reference_order() -> None:
    """Inspect the signature: parameters appear in the order
    ``(self, dbapi, connection)`` to match SA's reference connector."""
    sig = inspect.signature(AsyncAdaptedConnection.__init__)
    params = list(sig.parameters)
    assert params[:3] == ["self", "dbapi", "connection"], (
        f"AsyncAdaptedConnection.__init__ signature drifted from SA "
        f"reference shape (self, dbapi, connection): got {params}"
    )


def test_init_sa_reference_positional_construction_succeeds() -> None:
    """A caller copy-pasting ``AsyncAdapt_dbapi_connection(dbapi, raw)``
    from SA's reference example must not raise ``TypeError``. Pin the
    SA-reference positional shape end-to-end."""
    dbapi_module = MagicMock(name="dbapi_module")
    raw_conn = MagicMock(name="raw_conn")

    adapter = AsyncAdaptedConnection(dbapi_module, raw_conn)

    assert adapter.dbapi is dbapi_module
    assert adapter.driver_connection is raw_conn


def test_init_legacy_single_positional_still_works() -> None:
    """Backward compatibility: the legacy
    ``AsyncAdaptedConnection(raw_conn)`` single-positional shape keeps
    working. ``dbapi`` defaults to ``None`` in that path."""
    raw_conn = MagicMock(name="raw_conn")
    adapter = AsyncAdaptedConnection(raw_conn)
    assert adapter.driver_connection is raw_conn
    assert adapter.dbapi is None
