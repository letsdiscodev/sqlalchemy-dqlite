"""Async dialect: ``dqlite+aio://``, built on SQLAlchemy's asyncio adapter helpers."""

import asyncio
import collections
from typing import Any

from sqlalchemy import pool
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_terminate,
)
from sqlalchemy.engine import URL
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions as client_exc
import dqlitedbapi.exceptions as dbapi_exc
from dqlitedbapi import aio as dbapi_aio
from sqlalchemydqlite.base import AUTOCOMMIT_REJECTED, DqliteDialect

__all__ = ["AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"]


class AsyncAdaptedCursor(AsyncAdapt_dbapi_cursor):
    """Buffers the whole result at execute time; the dbapi cursor already does."""

    __slots__ = ()
    _awaitable_cursor_close = False

    def _aenter_cursor(self, cursor: Any) -> Any:
        return cursor

    async def _executemany_async(self, operation: Any, seq_of_parameters: Any) -> Any:
        async with self._adapt_connection._execute_mutex:
            result = await self._cursor.executemany(operation, seq_of_parameters)
            if self._cursor.description:  # executemany ... RETURNING
                self._rows = collections.deque(await self._cursor.fetchall())
            return result

    @property
    def lastrowid(self) -> int:
        return self._cursor.lastrowid

    def setinputsizes(self, *inputsizes: Any) -> None:
        return None


class AsyncAdaptedConnection(AsyncAdapt_terminate, AsyncAdapt_dbapi_connection):
    """Wraps :class:`dqlitedbapi.aio.AsyncConnection` for SQLAlchemy's greenlet bridge."""

    __slots__ = ()
    _cursor_cls = AsyncAdaptedCursor

    def cursor(self, server_side: bool = False) -> AsyncAdaptedCursor:
        if server_side:
            raise dbapi_exc.NotSupportedError(
                "Server-side cursors are not supported by the dqlite dialect"
            )
        return self._cursor_cls(self)

    @property
    def isolation_level(self) -> str:
        return "SERIALIZABLE"

    @property
    def autocommit(self) -> bool:
        return False

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if value:
            raise ArgumentError(AUTOCOMMIT_REJECTED)

    def close(self) -> None:
        try:
            self.await_(self._connection.close())
        except Exception as error:
            self._handle_exception(error)

    def _terminate_handled_exceptions(self) -> tuple[type[BaseException], ...]:
        return (
            TimeoutError,
            asyncio.CancelledError,
            OSError,
            RuntimeError,
            dbapi_exc.Error,
            client_exc.DqliteError,
        )

    async def _terminate_graceful_close(self) -> None:
        await self._connection.close()

    def _terminate_force_close(self) -> None:
        self._connection.force_close_transport()


class DqliteDialect_aio(DqliteDialect):
    """``create_async_engine("dqlite+aio://host:port/database")``."""

    driver = "aio"
    is_async = True
    supports_statement_cache = True
    _creator_kwarg = "async_creator_fn"

    @classmethod
    def import_dbapi(cls) -> Any:
        return dbapi_aio

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return pool.AsyncAdaptedQueuePool

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        raw = super().connect(*cargs, **cparams)
        try:
            AsyncAdaptedConnection.await_(raw.connect())
        except BaseException:
            raw.force_close_transport()
            raise
        return AsyncAdaptedConnection(self.loaded_dbapi, raw)

    def get_driver_connection(self, connection: Any) -> Any:
        return connection._connection

    def do_terminate(self, dbapi_connection: Any) -> None:
        dbapi_connection.terminate()
