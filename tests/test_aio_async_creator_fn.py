"""``connect_args={"async_creator_fn": ...}`` injects a custom factory.

SA convention (asyncpg.py:937, aiosqlite.py:399, aiomysql): callers
inject a pre-built async connection (or a wrapped factory) via
``connect_args={"async_creator_fn": my_factory}``. Pin behaviour:
- The pop precedes the kwarg-allowlist validation (the hook key is not
  on the allowlist).
- The default-factory ``loaded_dbapi.connect`` is NOT invoked when the
  hook is supplied.
- The two-step shape is preserved — ``raw_conn.connect()`` is awaited
  on the creator-returned object.

Tests run inside a greenlet (via ``greenlet_spawn``) because
``DqliteDialect_aio.connect`` calls ``await_only(...)`` which needs
SA's greenlet context.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _FakeConn:
    """Minimal stub satisfying the ``AsyncConnection`` shape that
    ``AsyncAdaptedConnection`` calls into."""

    def __init__(self) -> None:
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def rollback(self) -> None:
        pass


async def test_connect_honours_async_creator_fn() -> None:
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()  # would-be default factory

    called: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    fake = _FakeConn()

    def fake_creator(*args: Any, **kwargs: Any) -> _FakeConn:
        called.append((args, kwargs))
        return fake

    result = await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=fake_creator,
    )

    assert called == [(("1.2.3.4:9000",), {"database": "mydb"})]
    assert isinstance(result, AsyncAdaptedConnection)
    dialect.loaded_dbapi.connect.assert_not_called()
    assert fake.connected is True


async def test_connect_async_creator_fn_kwarg_does_not_trip_allowlist() -> None:
    """The kwarg pop must precede ``_validate_connect_kwargs``;
    otherwise the strict allowlist raises ``ArgumentError`` before the
    hook gets a chance.
    """
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    fake = _FakeConn()
    await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=lambda *a, **kw: fake,
    )
    # No raise — the test would fail with ArgumentError if the pop
    # were placed after allowlist validation.


async def test_connect_without_creator_uses_default_loaded_dbapi() -> None:
    """Regression guard: when the hook is absent, the existing
    ``loaded_dbapi.connect`` path runs unchanged."""
    dialect = DqliteDialect_aio()

    fake = _FakeConn()
    dialect.loaded_dbapi = MagicMock()
    dialect.loaded_dbapi.connect.return_value = fake

    result = await greenlet_spawn(dialect.connect, "1.2.3.4:9000", database="mydb")

    dialect.loaded_dbapi.connect.assert_called_once_with("1.2.3.4:9000", database="mydb")
    assert isinstance(result, AsyncAdaptedConnection)
    assert fake.connected is True


def test_connect_unknown_kwarg_still_raises_argumenterror() -> None:
    """Defence-in-depth: the allowlist still rejects truly unknown
    kwargs even with the new pop in place. ``_validate_connect_kwargs``
    runs before any ``await_only(...)`` call, so this raises
    synchronously and does not need a greenlet."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    with pytest.raises(ArgumentError):
        dialect.connect(
            "1.2.3.4:9000",
            database="mydb",
            unknown_kwarg_that_should_be_rejected=True,
        )
