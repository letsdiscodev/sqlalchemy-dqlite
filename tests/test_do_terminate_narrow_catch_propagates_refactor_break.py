"""Pin: ``do_terminate`` (sync/async) absorbs the full ``Exception`` surface per SA's
non-raising contract — DEBUG for expected transport shapes, WARNING for unexpected ones."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_sync_do_terminate_absorbs_attribute_error_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
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
    dialect = DqliteDialect()

    dbapi_connection = MagicMock()
    dbapi_connection.address = "host.cluster:9999"
    dbapi_connection.force_close_transport = MagicMock(side_effect=OSError("ECONNRESET"))

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
