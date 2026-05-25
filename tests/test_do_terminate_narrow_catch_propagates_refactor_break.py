"""Pin: ``DqliteDialect.do_terminate`` (sync) and
``DqliteDialect_aio.do_terminate`` (async) honour SA's
``has_terminate=True`` non-raising pool contract on the full
``Exception`` surface — DEBUG-log + absorb for expected transport-
class shapes (``_FORCE_CLOSE_TAIL_EXCEPTIONS``); WARNING-log +
absorb for unexpected shapes (cross-repo dbapi refactor regressions
like ``AttributeError`` from a rename).

The widened catch keeps the regression loudly observable in operator
logs (WARNING tier) without aborting ``engine.dispose()`` per SA's
binary non-raising contract.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_sync_do_terminate_absorbs_attribute_error_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A cross-repo dbapi refactor that removed
    ``force_close_transport`` surfaces as ``AttributeError``;
    SA's has_terminate=True contract requires absorption. WARNING-tier
    log keeps the regression visible."""
    dialect = DqliteDialect()

    dbapi_connection = MagicMock()
    dbapi_connection.address = "host.cluster:9999"
    dbapi_connection.force_close_transport = MagicMock(
        side_effect=AttributeError("simulated refactor: method renamed")
    )

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.base"):
        dialect.do_terminate(dbapi_connection)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected WARNING record for AttributeError surface"


def test_sync_do_terminate_still_absorbs_oserror_transport_class() -> None:
    """Sanity: legitimate transport-class failures stay absorbed
    (has_terminate=True non-raising contract)."""
    dialect = DqliteDialect()

    dbapi_connection = MagicMock()
    dbapi_connection.address = "host.cluster:9999"
    dbapi_connection.force_close_transport = MagicMock(side_effect=OSError("ECONNRESET"))

    # No raise — OSError is absorbed at the expected-shape tuple.
    dialect.do_terminate(dbapi_connection)


def test_async_do_terminate_absorbs_attribute_error_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dialect = DqliteDialect_aio()

    dbapi_connection = MagicMock()
    dbapi_connection.address = "host.cluster:9999"
    dbapi_connection.terminate = MagicMock(
        side_effect=AttributeError("simulated refactor: method renamed")
    )

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        dialect.do_terminate(dbapi_connection)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected WARNING record for AttributeError surface"


def test_async_do_terminate_still_absorbs_oserror_transport_class() -> None:
    dialect = DqliteDialect_aio()

    dbapi_connection = MagicMock()
    dbapi_connection.address = "host.cluster:9999"
    dbapi_connection.terminate = MagicMock(side_effect=OSError("ECONNRESET"))

    dialect.do_terminate(dbapi_connection)
