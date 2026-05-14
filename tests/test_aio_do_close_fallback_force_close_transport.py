"""Pin: the inherited ``DqliteDialect.do_close`` transport-class
fallback works against an ``AsyncAdaptedConnection`` too.

``DqliteDialect.do_close`` (base.py) handles a transport-class
exception during ``dbapi_connection.close()`` by falling back to
``dbapi_connection.force_close_transport()`` — public name, no
underscore. ``DqliteDialect_aio`` inherits this ``do_close``
unmodified.

Pre-fix the async-side ``AsyncAdaptedConnection`` only exposed
``_force_close_transport`` (underscore-prefixed). When SA's sync-
pool teardown path reached the transport-class fallback on an
async-adapted connection (cross-loop dispose), the fallback raised
``AttributeError`` (not in ``_TRANSPORT_CLASS_EXCEPTIONS``), which
escaped the ``contextlib.suppress``. The transport leaked; the
pool's forced-disposal contract broke.

Pin: an ``AsyncAdaptedConnection`` has a public
``force_close_transport`` attribute (alias of, or rename of, the
internal ``_force_close_transport``) so the inherited ``do_close``
fallback path completes symmetrically with the sync side, and a
transport-class first-close failure does NOT escape as
``AttributeError``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


def test_async_adapter_exposes_public_force_close_transport() -> None:
    """``AsyncAdaptedConnection`` must expose a public
    ``force_close_transport`` callable so the inherited ``do_close``
    fallback (which references the public name) resolves.
    """
    assert hasattr(AsyncAdaptedConnection, "force_close_transport"), (
        "AsyncAdaptedConnection must expose a public force_close_transport "
        "for the inherited DqliteDialect.do_close fallback to resolve"
    )
    attr = AsyncAdaptedConnection.force_close_transport
    assert callable(attr)


def test_public_force_close_transport_drives_internal_teardown() -> None:
    """The public ``force_close_transport`` must drive the same
    transport-teardown path as the existing ``_force_close_transport``.
    Wire a fake dbapi hook so the test does not depend on internal
    method identity — only on the observable effect.
    """
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
    """On a transport-class first-close failure, the inherited
    ``do_close`` falls through to the public ``force_close_transport``
    on the async adapter. No ``AttributeError`` escapes.
    """
    dialect = DqliteDialect_aio()

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "localhost:9001"
    inner.force_close_transport = MagicMock()
    adapter._connection = inner

    # Patch the class-level ``close`` to raise a transport-class
    # error on the first try — the same shape the cross-loop dispose
    # path surfaces in practice. The adapter uses ``__slots__`` so
    # instance-level assignment is disallowed; patch at the class
    # level for the test's scope.
    with patch.object(
        AsyncAdaptedConnection,
        "close",
        side_effect=transport_exc,
    ):
        # The fallback path must NOT escape as AttributeError. The
        # whole point of the fallback is to release the slot; an
        # AttributeError leak would break ``engine.dispose()``.
        dialect.do_close(adapter)

    # The fallback ran end-to-end: the dbapi hook (driven by the
    # public adapter method) fired exactly once.
    inner.force_close_transport.assert_called_once()
