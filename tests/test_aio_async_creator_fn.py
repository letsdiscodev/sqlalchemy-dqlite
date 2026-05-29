"""``connect_args={"async_creator_fn": ...}`` injects a custom factory.

Tests run inside a greenlet (``greenlet_spawn``) because ``connect`` calls ``await_only``.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import AsyncAdaptedConnection, DqliteDialect_aio


class _FakeConn:
    """Minimal stub satisfying the ``AsyncConnection`` shape."""

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
    dialect.loaded_dbapi = MagicMock()

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
    """The kwarg pop must precede ``_validate_connect_kwargs`` (the key is not allowlisted)."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    fake = _FakeConn()
    await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=lambda *a, **kw: fake,
    )


async def test_connect_without_creator_uses_default_loaded_dbapi() -> None:
    """Regression guard: when the hook is absent, the ``loaded_dbapi.connect`` path runs."""
    dialect = DqliteDialect_aio()

    fake = _FakeConn()
    dialect.loaded_dbapi = MagicMock()
    dialect.loaded_dbapi.connect.return_value = fake

    result = await greenlet_spawn(dialect.connect, "1.2.3.4:9000", database="mydb")

    dialect.loaded_dbapi.connect.assert_called_once_with("1.2.3.4:9000", database="mydb")
    assert isinstance(result, AsyncAdaptedConnection)
    assert fake.connected is True


async def test_async_creator_fn_idempotent_connect_safe_to_double_call() -> None:
    """The dialect always awaits ``raw_conn.connect()`` after the creator runs; an
    already-connected creator result with an idempotent ``connect()`` must not double-open."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    connect_calls = [0]

    class IdempotentFakeConn(_FakeConn):
        async def connect(self) -> None:
            connect_calls[0] += 1
            if connect_calls[0] == 1:
                self.connected = True

    fake = IdempotentFakeConn()
    # Pre-connect to simulate a creator that opens before returning.
    await fake.connect()
    assert connect_calls[0] == 1
    assert fake.connected is True

    await greenlet_spawn(
        dialect.connect,
        "1.2.3.4:9000",
        database="mydb",
        async_creator_fn=lambda *a, **kw: fake,
    )

    # Two connect() calls: the test's pre-call plus the dialect's unconditional re-call.
    assert connect_calls[0] == 2
    assert fake.connected is True


def test_connect_unknown_kwarg_still_raises_argumenterror() -> None:
    """The allowlist still rejects unknown kwargs; this raises synchronously (no greenlet)."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    with pytest.raises(ArgumentError):
        dialect.connect(
            "1.2.3.4:9000",
            database="mydb",
            unknown_kwarg_that_should_be_rejected=True,
        )
