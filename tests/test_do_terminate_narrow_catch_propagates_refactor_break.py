"""Pin: ``DqliteDialect.do_terminate`` (sync) and
``DqliteDialect_aio.do_terminate`` (async) catch the narrow
``_FORCE_CLOSE_TAIL_EXCEPTIONS`` tuple rather than bare ``Exception``.

The narrow tuple still upholds SA's ``has_terminate=True`` non-raising
contract for real transport-class failures, but lets cross-repo
refactor breaks (``AttributeError`` / ``TypeError``) propagate so the
regression surfaces loudly rather than silent no-op on every
``engine.dispose()``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_sync_do_terminate_propagates_attribute_error_from_refactor() -> None:
    dialect = DqliteDialect()

    dbapi_connection = MagicMock()
    # Simulate a future dbapi refactor that removed force_close_transport.
    dbapi_connection.force_close_transport = MagicMock(
        side_effect=AttributeError("simulated refactor: method renamed")
    )

    with pytest.raises(AttributeError, match="simulated refactor"):
        dialect.do_terminate(dbapi_connection)


def test_sync_do_terminate_still_absorbs_oserror_transport_class() -> None:
    """Sanity: legitimate transport-class failures stay absorbed
    (has_terminate=True non-raising contract)."""
    dialect = DqliteDialect()

    dbapi_connection = MagicMock()
    dbapi_connection.force_close_transport = MagicMock(side_effect=OSError("ECONNRESET"))

    # No raise — OSError is absorbed at the narrow tuple.
    dialect.do_terminate(dbapi_connection)


def test_async_do_terminate_propagates_attribute_error_from_refactor() -> None:
    dialect = DqliteDialect_aio()

    dbapi_connection = MagicMock()
    dbapi_connection.terminate = MagicMock(
        side_effect=AttributeError("simulated refactor: method renamed")
    )

    with pytest.raises(AttributeError, match="simulated refactor"):
        dialect.do_terminate(dbapi_connection)


def test_async_do_terminate_still_absorbs_oserror_transport_class() -> None:
    dialect = DqliteDialect_aio()

    dbapi_connection = MagicMock()
    dbapi_connection.terminate = MagicMock(side_effect=OSError("ECONNRESET"))

    dialect.do_terminate(dbapi_connection)
