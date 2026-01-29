"""Async dqlite dialect for SQLAlchemy."""

from collections import deque
from collections.abc import Sequence
from typing import Any

from sqlalchemy import pool
from sqlalchemy.engine import URL, AdaptedConnection
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util import await_only

from sqlalchemydqlite.base import DqliteDialect


class AsyncAdaptedCursor:
    """Adapts an AsyncCursor for SQLAlchemy's greenlet-based async engine.

    Eagerly fetches all rows during execute() within the greenlet context,
    then serves fetch* calls synchronously from the buffer. This matches
    the pattern used by SQLAlchemy's aiosqlite dialect.
    """

    server_side = False

    def __init__(self, adapt_connection: "AsyncAdaptedConnection") -> None:
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.description: Any = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.arraysize: int = 1
        self._rows: deque[Any] = deque()

    async def _async_soft_close(self) -> None:
        return

    def close(self) -> None:
        self._rows.clear()

    def execute(self, operation: str, parameters: Any = None) -> Any:
        cursor = self._connection.cursor()
        if parameters is not None:
            await_only(cursor.execute(operation, parameters))
        else:
            await_only(cursor.execute(operation))

        if cursor.description:
            self.description = cursor.description
            self.lastrowid = self.rowcount = -1
            self._rows = deque(await_only(cursor.fetchall()))
        else:
            self.description = None
            self.lastrowid = cursor.lastrowid
            self.rowcount = cursor.rowcount

        await_only(cursor.close())

    def executemany(self, operation: str, seq_of_parameters: Any) -> Any:
        cursor = self._connection.cursor()
        await_only(cursor.executemany(operation, seq_of_parameters))
        self.description = None
        self.lastrowid = cursor.lastrowid
        self.rowcount = cursor.rowcount
        await_only(cursor.close())

    def fetchone(self) -> Any:
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size: int | None = None) -> Sequence[Any]:
        if size is None:
            size = self.arraysize
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> Sequence[Any]:
        retval = list(self._rows)
        self._rows.clear()
        return retval

    def setinputsizes(self, *inputsizes: Any) -> None:
        pass

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        pass

    def __iter__(self) -> Any:
        while self._rows:
            yield self._rows.popleft()

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class AsyncAdaptedConnection(AdaptedConnection):
    """Adapts an AsyncConnection for SQLAlchemy's greenlet-based async engine.

    Provides sync-looking methods that internally use await_only() to
    bridge to the underlying async connection within SQLAlchemy's
    greenlet context.
    """

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def cursor(self) -> AsyncAdaptedCursor:
        return AsyncAdaptedCursor(self)

    def commit(self) -> None:
        await_only(self._connection.commit())

    def rollback(self) -> None:
        await_only(self._connection.rollback())

    def close(self) -> None:
        await_only(self._connection.close())


class DqliteDialect_aio(DqliteDialect):  # noqa: N801
    """Async SQLAlchemy dialect for dqlite.

    Use with SQLAlchemy's async engine:
        create_async_engine("dqlite+aio://host:port/database")
    """

    driver = "dqlitedbapi_aio"
    is_async = True
    supports_statement_cache = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(cls) -> Any:
        from dqlitedbapi import aio

        return aio

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Create and wrap an async connection."""
        raw_conn = self.loaded_dbapi.connect(*cargs, **cparams)
        return AsyncAdaptedConnection(raw_conn)

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from URL.

        URL format: dqlite+aio://host:port/database
        """
        host = url.host or "localhost"
        port = url.port or 9001
        database = url.database or "default"

        address = f"{host}:{port}"

        return [], {
            "address": address,
            "database": database,
        }

    def do_rollback(self, dbapi_connection: DBAPIConnection) -> None:
        """Rollback the current transaction."""
        try:
            dbapi_connection.rollback()
        except Exception as e:
            if "no transaction is active" not in str(e):
                raise

    def do_commit(self, dbapi_connection: DBAPIConnection) -> None:
        """Commit the current transaction."""
        try:
            dbapi_connection.commit()
        except Exception as e:
            if "no transaction is active" not in str(e):
                raise

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return the server version as a tuple."""
        cursor = connection.connection.dbapi_connection.cursor()
        cursor.execute("SELECT sqlite_version()")
        row = cursor.fetchone()
        cursor.close()

        if row:
            version_str = row[0]
            return tuple(int(x) for x in version_str.split("."))
        return (3, 0, 0)

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the driver-level connection."""
        return connection


# Register the dialect
dialect = DqliteDialect_aio
