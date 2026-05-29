"""Pin: ``connect()`` constructs ``raw_conn`` inside the ``try`` frame so a
``BaseException`` from the construction routes through cleanup, not an orphan leak."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio


class _Boom(BaseException):
    pass


async def test_connect_propagates_baseexception_from_creator_fn() -> None:
    """raw_conn is still None, so the cleanup arm short-circuits without UnboundLocalError."""
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    def creator() -> Any:
        raise _Boom("construction failed")

    with pytest.raises(_Boom):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)


async def test_connect_propagates_baseexception_from_dbapi_connect() -> None:
    """Default-factory path: ``loaded_dbapi.connect`` raising must propagate cleanly."""
    dialect = DqliteDialect_aio()

    def boom(*_a: Any, **_kw: Any) -> Any:
        raise _Boom("dbapi connect failed")

    fake_dbapi = MagicMock()
    fake_dbapi.connect = boom
    dialect.loaded_dbapi = fake_dbapi

    with pytest.raises(_Boom):
        await greenlet_spawn(dialect.connect)


async def test_connect_cleanup_arm_handles_raw_conn_none() -> None:
    """The ``raw_conn is not None`` guard must short-circuit before NameError."""
    dialect = DqliteDialect_aio()

    def creator() -> Any:
        raise RuntimeError("creator failed before binding raw_conn")

    with pytest.raises(RuntimeError, match="creator failed"):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)


async def test_connect_cleanup_arm_terminates_after_successful_construction() -> None:
    """When ``raw_conn`` is bound but ``connect()`` raises, cleanup must terminate it."""

    class _ConnRaises:
        def __init__(self) -> None:
            self.terminate_called = False

        async def connect(self) -> None:
            raise RuntimeError("inner connect failed")

        async def close(self) -> None:
            self.terminate_called = True

        def force_close_transport(self) -> None:
            self.terminate_called = True

    raw = _ConnRaises()
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    def creator() -> Any:
        return raw

    with pytest.raises(RuntimeError, match="inner connect failed"):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)

    assert raw.terminate_called is True
