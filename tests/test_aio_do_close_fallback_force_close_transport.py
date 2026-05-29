"""Pin: ``AsyncAdaptedConnection`` exposes a public ``force_close_transport`` so the inherited
``DqliteDialect.do_close`` transport-class fallback completes instead of escaping AttributeError."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


def test_async_adapter_exposes_public_force_close_transport() -> None:
    """The public ``force_close_transport`` callable must exist for the inherited fallback."""
    assert hasattr(AsyncAdaptedConnection, "force_close_transport"), (
        "AsyncAdaptedConnection must expose a public force_close_transport "
        "for the inherited DqliteDialect.do_close fallback to resolve"
    )
    attr = AsyncAdaptedConnection.force_close_transport
    assert callable(attr)


def test_public_force_close_transport_drives_internal_teardown() -> None:
    """The public ``force_close_transport`` drives the same transport-teardown path."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.force_close_transport = MagicMock()
    adapter._connection = inner

    adapter.force_close_transport()

    inner.force_close_transport.assert_called_once()


@pytest.mark.parametrize(
    "transport_exc",
    [
        _dbapi_exc.OperationalError("close timed out"),
        _dbapi_exc.InterfaceError("interface error"),
        _client_exc.DqliteConnectionError("transport gone"),
        OSError("close OSError"),
    ],
    ids=["OperationalError", "InterfaceError", "DqliteConnectionError", "OSError"],
)
def test_async_do_close_transport_failure_falls_through_to_force_close(
    transport_exc: BaseException,
) -> None:
    """On a transport-class first-close failure, do_close falls through to force_close_transport
    with no AttributeError escape."""
    dialect = DqliteDialect_aio()

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.force_close_transport = MagicMock()
    adapter._connection = inner

    # Patch at the class level: the adapter uses __slots__ so instance-level assignment fails.
    with patch.object(
        AsyncAdaptedConnection,
        "close",
        side_effect=transport_exc,
    ):
        dialect.do_close(adapter)

    inner.force_close_transport.assert_called_once()
