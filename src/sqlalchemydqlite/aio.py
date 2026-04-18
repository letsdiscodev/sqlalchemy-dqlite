"""Async dqlite dialect for SQLAlchemy."""

import contextlib
from collections import deque
from collections.abc import Sequence
from typing import Any

from sqlalchemy import pool
from sqlalchemy.engine import URL, AdaptedConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util import await_only

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import InterfaceError, OperationalError
from sqlalchemydqlite.base import DqliteDialect

__all__ = ["DqliteDialect_aio"]


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
        # Clear buffered state FIRST so a CancelledError (or any other
        # exception) during execute/fetchall leaves the adapter in a
        # "no active result" state rather than carrying stale rows
        # from a previous execution.
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows.clear()

        cursor = self._connection.cursor()
        try:
            if parameters is not None:
                await_only(cursor.execute(operation, parameters))
            else:
                await_only(cursor.execute(operation))

            if cursor.description:
                self.description = cursor.description
                self._rows = deque(await_only(cursor.fetchall()))
            else:
                self.lastrowid = cursor.lastrowid
                self.rowcount = cursor.rowcount
        finally:
            await_only(cursor.close())

    def executemany(self, operation: str, seq_of_parameters: Any) -> Any:
        # Clear state up-front so cancellation mid-call doesn't leak
        # a previous execution's buffered rows.
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows.clear()

        cursor = self._connection.cursor()
        try:
            await_only(cursor.executemany(operation, seq_of_parameters))
            self.lastrowid = cursor.lastrowid
            self.rowcount = cursor.rowcount
        finally:
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
        # Attempt rollback before close so a caller that exits without
        # committing does not leave a dangling server-side transaction.
        # The underlying async connection's rollback is a silent no-op when
        # no transaction is active and when the connection has never been
        # used, so the double-call is safe.
        #
        # Narrow the suppression to the categories a best-effort rollback
        # can legitimately raise — connection-level / transport errors —
        # so programming bugs (AttributeError, TypeError, bare RuntimeError,
        # etc.) still propagate. The tuple mirrors the client layer's own
        # is_disconnect classification in base.py.
        with contextlib.suppress(
            OperationalError,
            InterfaceError,
            DqliteConnectionError,
            OSError,
            TimeoutError,
            ConnectionError,
        ):
            await_only(self._connection.rollback())
        await_only(self._connection.close())


class DqliteDialect_aio(DqliteDialect):  # noqa: N801
    """Async SQLAlchemy dialect for dqlite.

    Use with SQLAlchemy's async engine:
        create_async_engine("dqlite+aio://host:port/database")
    """

    driver = "dqlitedbapi_aio"
    is_async = True
    # MUST be redeclared here even though the base class already sets it to
    # True: SQLAlchemy reads this attribute via
    # ``self.__class__.__dict__.get("supports_statement_cache")`` (see
    # engine/default.py::_supports_statement_cache), which is a
    # single-class lookup, not an MRO lookup. If this line is removed,
    # statement caching is silently disabled on the async dialect and a
    # warning fires on every engine startup.
    supports_statement_cache = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(cls) -> Any:
        from dqlitedbapi import aio

        return aio

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Create and wrap an async connection.

        Eagerly establishes the TCP connection so errors surface at
        connect-time rather than on the first query.
        """
        raw_conn = self.loaded_dbapi.connect(*cargs, **cparams)
        await_only(raw_conn.connect())
        return AsyncAdaptedConnection(raw_conn)

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the underlying driver-level connection."""
        return connection._connection


# Register the dialect
dialect = DqliteDialect_aio
