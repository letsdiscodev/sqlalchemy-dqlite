"""Pin: ``DqliteDialect.connect()`` validates the merged
``cparams`` against ``_CONNECT_KWARG_ALLOWED`` so a typo in
``create_engine(connect_args={...})`` raises ``ArgumentError`` —
matching the URL query path's error class — instead of deferring to
``dqlitedbapi.connect``'s ``NotSupportedError`` at first checkout.

The URL query path is gated by ``_URL_QUERY_ALLOWED`` and rejects
typos at engine construction; the ``connect_args=`` path bypasses
that gate because SA's ``cparams.union(connect_args)`` happens AFTER
``create_connect_args`` returns. Pin both paths' validation so the
allowlist is symmetric across the two surfaces.
"""

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
    """Positive control: every ``_CONNECT_KWARG_ALLOWED`` entry can
    flow through ``connect()``."""
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
    """The ArgumentError message must enumerate the allowed keys so
    the user can correct the typo without consulting the source."""
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
    """Defence pin: the dialect's ``_CONNECT_KWARG_ALLOWED`` must be a
    subset of (or equal to) the actual dbapi.connect signature. A
    future dbapi signature change must propagate here."""
    import inspect

    import dqlitedbapi

    sig = inspect.signature(dqlitedbapi.connect)
    dbapi_keys = {p.name for p in sig.parameters.values() if p.name != "unknown_kwargs"}
    # Every allowlist entry must be a real dbapi.connect parameter.
    for key in DqliteDialect._CONNECT_KWARG_ALLOWED:
        assert key in dbapi_keys, (
            f"_CONNECT_KWARG_ALLOWED has stale key {key!r} not in dqlitedbapi.connect signature"
        )
