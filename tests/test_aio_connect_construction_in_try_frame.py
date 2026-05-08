"""Pin: ``DqliteDialect_aio.connect()`` constructs ``raw_conn`` inside
the ``try`` frame so a ``BaseException`` delivered by the construction
itself routes through the cleanup arm rather than leaking an orphan
``AsyncConnection``.

Without the construct-inside-try discipline, a creator factory that
raises ``BaseException`` (KeyboardInterrupt / SystemExit window) before
returning leaks the freshly-built object — registered loop locks plus
``weakref.finalize`` warning surface — without orderly cleanup. The fix
threads the construction inside the same try frame as the ``connect()``
await so the cleanup arm runs whenever ``raw_conn`` was bound.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from sqlalchemydqlite.aio import DqliteDialect_aio


class _Boom(BaseException):
    pass


async def test_connect_propagates_baseexception_from_creator_fn() -> None:
    """The creator factory raising ``BaseException`` must propagate
    untouched. With the construction inside the try frame, ``raw_conn``
    is still ``None`` so the cleanup arm short-circuits and we re-raise
    cleanly without an ``UnboundLocalError`` on the ``raw_conn``
    reference.
    """
    dialect = DqliteDialect_aio()
    dialect.loaded_dbapi = MagicMock()

    def creator() -> Any:
        raise _Boom("construction failed")

    with pytest.raises(_Boom):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)


async def test_connect_propagates_baseexception_from_dbapi_connect() -> None:
    """Default-factory path: ``loaded_dbapi.connect`` raising
    ``BaseException`` must also propagate cleanly.
    """
    dialect = DqliteDialect_aio()

    def boom(*_a: Any, **_kw: Any) -> Any:
        raise _Boom("dbapi connect failed")

    fake_dbapi = MagicMock()
    fake_dbapi.connect = boom
    dialect.loaded_dbapi = fake_dbapi

    with pytest.raises(_Boom):
        await greenlet_spawn(dialect.connect)


async def test_connect_cleanup_arm_handles_raw_conn_none() -> None:
    """The cleanup arm must not ``NameError`` on ``raw_conn`` when
    construction itself raises (covered by the ``raw_conn is not None``
    guard added when the construction moved inside the try frame).
    """
    dialect = DqliteDialect_aio()

    def creator() -> Any:
        # Raise an Exception that the cleanup arm would otherwise want
        # to call ``terminate()`` on. The guard must short-circuit
        # before referencing ``raw_conn``.
        raise RuntimeError("creator failed before binding raw_conn")

    with pytest.raises(RuntimeError, match="creator failed"):
        await greenlet_spawn(dialect.connect, async_creator_fn=creator)


async def test_connect_cleanup_arm_terminates_after_successful_construction() -> None:
    """When ``raw_conn`` is bound but ``connect()`` raises, the
    cleanup arm must invoke ``terminate()`` on the AsyncAdaptedConnection
    wrapping the bound raw_conn — i.e., the fix does not regress the
    pre-existing cleanup discipline.
    """

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
