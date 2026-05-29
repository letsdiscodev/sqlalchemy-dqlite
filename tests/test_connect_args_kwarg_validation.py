"""``DqliteDialect.connect()`` validates merged ``cparams`` against
``_CONNECT_KWARG_ALLOWED`` so a connect_args typo raises ``ArgumentError``
(matching the URL path). connect_args bypasses ``_URL_QUERY_ALLOWED``
since SA unions it after ``create_connect_args`` returns."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_dialect_connect_rejects_unknown_kwarg() -> None:
    dialect = DqliteDialect()
    dialect.loaded_dbapi = MagicMock()
    with pytest.raises(ArgumentError, match="Unknown dqlite connect kwarg"):
        dialect.connect("host:9001", database="main", timeoutt=5)


def test_dialect_connect_accepts_known_kwargs() -> None:
    """Every ``_CONNECT_KWARG_ALLOWED`` entry flows through ``connect()``."""
    dialect = DqliteDialect()
    dialect.loaded_dbapi = MagicMock()
    dialect.loaded_dbapi.connect.return_value = MagicMock()
    dialect.connect(
        "host:9001",
        database="main",
        timeout=5.0,
        max_total_rows=1000,
        max_continuation_frames=100,
        trust_server_heartbeat=False,
        close_timeout=0.5,
    )
    dialect.loaded_dbapi.connect.assert_called_once()


def test_dialect_connect_kwarg_message_lists_allowed_keys() -> None:
    """The error message enumerates the allowed keys."""
    dialect = DqliteDialect()
    dialect.loaded_dbapi = MagicMock()
    with pytest.raises(ArgumentError) as exc_info:
        dialect.connect("host:9001", timeoutt=5)
    msg = str(exc_info.value)
    assert "timeout" in msg
    assert "max_total_rows" in msg


def test_async_dialect_connect_rejects_unknown_kwarg() -> None:
    """Async sibling pin — same allowlist, same diagnostic."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()
    with pytest.raises(ArgumentError, match="Unknown dqlite connect kwarg"):
        dialect.connect("host:9001", timeoutt=5)


def test_dialect_connect_kwargs_allowlist_matches_dbapi_signature() -> None:
    """``_CONNECT_KWARG_ALLOWED`` must be a subset of the actual
    dbapi.connect signature so a signature change propagates here."""
    import inspect

    import dqlitedbapi

    sig = inspect.signature(dqlitedbapi.connect)
    dbapi_keys = {p.name for p in sig.parameters.values() if p.name != "unknown_kwargs"}
    for key in DqliteDialect._CONNECT_KWARG_ALLOWED:
        assert key in dbapi_keys, (
            f"_CONNECT_KWARG_ALLOWED has stale key {key!r} not in dqlitedbapi.connect signature"
        )
