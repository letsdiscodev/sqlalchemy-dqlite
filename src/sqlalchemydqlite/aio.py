"""Async dqlite dialect for SQLAlchemy."""

import asyncio
import contextlib
import logging
import types
from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import pool
from sqlalchemy.engine import URL, AdaptedConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util import await_only

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import (
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitedbapi.types import _DescriptionTuple
from sqlalchemydqlite.base import DqliteDialect

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dqlitedbapi.aio import AsyncConnection

__all__ = ["AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"]

# PEP 249 specifies ``cursor.description`` as a sequence of sequences —
# a ``list[tuple]`` is the canonical shape but a strict type alias of
# ``list`` would reject a dbapi cursor that returns a tuple-of-tuples
# (which sqlalchemy's own aiosqlite adapter accepts). Widen the outer
# alias to ``Sequence`` so the adapter passes through whatever the
# underlying cursor returns without copying. The inner 7-tuple shape is
# imported from the dbapi layer (single source of truth) so a future
# column (real display_size, etc.) propagates here automatically.
_Description = Sequence[_DescriptionTuple] | None


class AsyncAdaptedCursor:
    """Adapts an AsyncCursor for SQLAlchemy's greenlet-based async engine.

    Eagerly fetches all rows during execute() within the greenlet context,
    then serves fetch* calls synchronously from the buffer. This matches
    the pattern used by SQLAlchemy's aiosqlite dialect.

    .. note::
       ``arraysize`` on this adapter controls ONLY the chunk size
       returned by :meth:`fetchmany` from the already-buffered deque —
       it has **no effect on memory footprint**. ``execute()`` /
       ``executemany()`` unconditionally call ``fetchall()`` on the
       underlying ``AsyncCursor`` within the greenlet context, so the
       full result set is materialised in memory before any
       ``fetchmany`` call runs. Tuning ``arraysize`` to cap memory —
       the standard PEP 249 idiom — does not work through the adapter.
       Callers that need streaming-memory semantics must use
       :class:`dqlitedbapi.aio.AsyncCursor` directly and drive it from
       native async code; the greenlet-eager-fetch pattern is a
       deliberate part of how SA's async engine works, not a
       per-dialect choice.
    """

    # Declare instance layout — matches the slot discipline SA's own
    # ``AsyncAdapt_aiosqlite_cursor`` uses. Each execute() constructs
    # a fresh adapter cursor, so the per-instance ``__dict__`` overhead
    # is load-bearing under a busy engine.
    __slots__ = (
        "_adapt_connection",
        "_arraysize",
        "_closed",
        "_connection",
        "_rows",
        "description",
        "lastrowid",
        "rowcount",
    )

    server_side = False

    def __init__(self, adapt_connection: "AsyncAdaptedConnection") -> None:
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.description: _Description = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self._arraysize: int = 1
        self._rows: deque[Any] = deque()
        # PEP 249: after ``close()`` the cursor is unusable. Track the
        # flag so setinputsizes / setoutputsize can honour the contract
        # — the underlying AsyncCursor already raises InterfaceError
        # on the closed-cursor misuse and the adapter's silent
        # no-op would otherwise hide the bug from callers migrating
        # between the two cursor types.
        self._closed: bool = False

    async def _async_soft_close(self) -> None:
        return

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Validated setter mirroring ``dqlitedbapi.Cursor.arraysize``.

        Rejects ``bool``, non-int, and ``< 1`` so the ``arraysize=0`` /
        ``arraysize=-1`` footguns are caught at the assignment rather
        than silently turning every ``fetchmany`` call into ``[]`` (which
        makes ``while batch := cursor.fetchmany(): ...`` skip the entire
        result set).
        """
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProgrammingError(f"arraysize must be a positive integer, got {value!r}")
        if value < 1:
            raise ProgrammingError(f"arraysize must be >= 1, got {value}")
        self._arraysize = value

    def close(self) -> None:
        self._rows.clear()
        self._closed = True

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
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
                # Fetch first, assign atomically. If ``fetchall`` raises
                # (CancelledError from an outer timeout, server fault
                # mid-stream, etc.), ``self.description`` must not be left
                # set while ``self._rows`` is still empty — SQLAlchemy's
                # Result layer treats (description, empty rows) as an
                # empty result set, indistinguishable from "execute
                # succeeded but fetched no rows".
                fetched = deque(await_only(cursor.fetchall()))
                self.description = cursor.description
                self._rows = fetched
                # Mirror the DML branch: rowcount / lastrowid are set by
                # the underlying cursor on the RETURNING path too
                # (rowcount = len(rows); lastrowid from the last
                # INSERT). SQLAlchemy's Result layer reads both through
                # the adapter, so leaving rowcount at -1 would silently
                # collapse "N rows returned" into "not determinable".
                self.rowcount = cursor.rowcount
                self.lastrowid = cursor.lastrowid
            else:
                self.lastrowid = cursor.lastrowid
                self.rowcount = cursor.rowcount
        finally:
            # ``cursor.close`` is in-memory state-clearing only; a
            # failure here has no external effect. Suppressing it keeps
            # any primary exception (execute / fetchall raise) the
            # active one rather than being replaced by a secondary
            # close-time error. Narrow to ``(Exception,
            # asyncio.CancelledError)`` so a greenlet-level cancel is
            # still covered (``CancelledError`` subclasses
            # ``BaseException`` since 3.8) but ``KeyboardInterrupt`` /
            # ``SystemExit`` propagate — the stdlib's own
            # ``contextlib.suppress`` docs call out ``BaseException``
            # here as an anti-pattern for exactly this reason.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await_only(cursor.close())

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        # Clear state up-front so cancellation mid-call doesn't leak
        # a previous execution's buffered rows.
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows.clear()

        cursor = self._connection.cursor()
        try:
            await_only(cursor.executemany(operation, seq_of_parameters))
            # Mirror execute()'s post-call pattern: if the statement had
            # a RETURNING clause, the underlying cursor accumulates rows
            # across parameter sets and sets a description. Skipping the
            # description/rows capture silently loses every returned row
            # when SQLAlchemy's insertmanyvalues + RETURNING path is
            # driven through the async engine.
            if cursor.description:
                # Same fetch-first-then-assign pattern as ``execute``:
                # a raise from ``fetchall`` must not leave description
                # populated with empty rows.
                fetched = deque(await_only(cursor.fetchall()))
                self.description = cursor.description
                self._rows = fetched
                # Mirror execute()'s RETURNING path: rowcount /
                # lastrowid are accumulated by the underlying cursor
                # across parameter sets and must flow through the
                # adapter so SQLAlchemy's Result layer sees them.
                self.rowcount = cursor.rowcount
                self.lastrowid = cursor.lastrowid
            else:
                self.lastrowid = cursor.lastrowid
                self.rowcount = cursor.rowcount
        finally:
            # Same narrow suppression as ``execute``'s finally block
            # above — see the rationale there. Keeps KI / SystemExit
            # propagating while still covering greenlet cancellation.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await_only(cursor.close())

    def fetchone(self) -> Any | None:
        # Narrow from ``Any`` so callers understand None is a legitimate
        # return on exhaustion (PEP 249 contract, mirroring the
        # dqlitedbapi sync / async cursors that already type this as
        # ``tuple[Any, ...] | None``). Runtime behaviour unchanged.
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size: int | None = None) -> Sequence[Any]:
        if size is None:
            size = self.arraysize
        if size < 0:
            raise ProgrammingError(f"fetchmany size must be non-negative, got {size}")
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> Sequence[Any]:
        retval = list(self._rows)
        self._rows.clear()
        return retval

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        # PEP 249: called before execute*() to hint bind-parameter sizes.
        # dqlite's wire encoder does not use per-parameter sizing hints,
        # so the implementation is a no-op on an open cursor — but the
        # closed-cursor case must raise to match the underlying
        # AsyncCursor's behaviour and to keep ``is_disconnect``'s
        # narrow "cursor is closed" InterfaceError branch reachable
        # through the adapter.
        if self._closed:
            raise InterfaceError("cursor is closed")

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")

    @property
    def connection(self) -> "AsyncAdaptedConnection":
        """The AsyncAdaptedConnection this cursor was created from.

        PEP 249 optional extension mirroring Cursor.connection /
        AsyncCursor.connection. Read-only.
        """
        return self._adapt_connection

    # PEP 249 optional extensions. The non-adapter cursors raise
    # NotSupportedError for these same calls; do the same here so a
    # consumer catching NotSupportedError behaves consistently whether it
    # is handed an AsyncCursor or a greenlet-wrapped AsyncAdaptedCursor.
    #
    # `rownumber` is deliberately NOT implemented: the adapter buffers
    # rows into a deque that is popped left on consumption, so a truthful
    # counter would need parallel state increments in fetchone /
    # fetchmany / fetchall / __next__. Consumers who need rownumber
    # should use AsyncCursor directly.
    def callproc(
        self, procname: str, parameters: Sequence[Any] | None = None
    ) -> Sequence[Any] | None:
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> bool | None:
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> None:
        raise NotSupportedError("dqlite cursors are not scrollable")

    def __iter__(self) -> Iterator[Any]:
        # Return self so ``iter(cursor) is cursor`` — PEP 234 iterator
        # protocol. The previous generator body (``while self._rows:
        # yield self._rows.popleft()``) produced a fresh generator each
        # time and split iteration into two incompatible paths: the
        # generator popped rows directly while ``__next__`` routed
        # through ``fetchone``. ``__next__`` now drives iteration for
        # both ``for row in cursor`` and ``next(cursor)``; the sibling
        # cursors ``dqlitedbapi.Cursor`` and ``AsyncCursor`` already
        # follow this pattern.
        return self

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

    # Parent ``sqlalchemy.engine.interfaces.AdaptedConnection`` declares
    # ``__slots__ = ("_connection",)``; without our own slots declaration
    # each instance gets a ``__dict__`` and defeats the parent's memory
    # optimization (SA's own ``AsyncAdapt_aiosqlite_connection`` follows
    # the same pattern). We add no new instance attributes, so an empty
    # slots tuple is correct.
    __slots__ = ()

    def __init__(self, connection: "AsyncConnection") -> None:
        # ``_connection`` is the concrete ``dqlitedbapi.aio.AsyncConnection``
        # this adapter wraps; SQLAlchemy's parent ``AdaptedConnection``
        # declares the attribute with a wider Protocol type, so we keep
        # the store on ``Any`` and rely on the annotation here to document
        # the intended input shape.
        self._connection: Any = connection

    def cursor(self) -> AsyncAdaptedCursor:
        return AsyncAdaptedCursor(self)

    @property
    def isolation_level(self) -> str:
        """Report the only level dqlite honours: ``"SERIALIZABLE"``.

        dqlite runs every statement through Raft consensus; there is no
        mechanism to weaken isolation. SA's reference aiosqlite adapter
        exposes ``isolation_level`` as a read/write property backed by
        the underlying connection, and SA diagnostics / third-party
        middleware probe ``getattr(dbapi_conn, "isolation_level",
        None)`` on several code paths. Without this property those
        probes would see ``None`` and either log "isolation unknown"
        or bypass a pin. Read-only: SA's engine flow already
        short-circuits ``set_isolation_level`` to accept only
        ``"SERIALIZABLE"``, so there is no setter surface to proxy.
        """
        return "SERIALIZABLE"

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
        # etc.) still propagate. ``ConnectionError``, ``BrokenPipeError``,
        # and ``TimeoutError`` are all ``OSError`` subclasses (since
        # Python 3.3+/3.10+ respectively), so a single ``OSError`` check
        # covers every stdlib transport-error shape — matching the
        # source-of-truth classification in ``base.py``'s
        # ``is_disconnect``.
        #
        # Wrap in ``try/finally`` so close() runs regardless of how
        # rollback() exits — narrow-caught, programming bug, or
        # ``BaseException`` like ``CancelledError`` during pool dispose.
        # SA's pool does not re-call close() on failure, so skipping
        # close would leak the underlying AsyncConnection. Mirror of the
        # inverse leak fixed in DqliteDialect_aio.connect().
        try:
            try:
                await_only(self._connection.rollback())
            except (
                OperationalError,
                InterfaceError,
                DqliteConnectionError,
                OSError,
            ) as exc:
                # Silent suppression used to hide e.g. "leader flip
                # mid-rollback" from operators — a DEBUG line preserves
                # the diagnostic without masking or propagating. Include
                # both id(self) and the peer address so a noisy pool can
                # be correlated to specific adapter instances and nodes.
                peer = getattr(self._connection, "address", None)
                logger.debug(
                    "AsyncAdaptedConnection.close (id=%s, peer=%s): "
                    "rollback failed (%s); proceeding to close",
                    id(self),
                    peer,
                    type(exc).__name__,
                    exc_info=True,
                )
        finally:
            # Narrow the close-time exception set to transport-class
            # failures. A transient OSError / DqliteConnectionError
            # mid-close must not escape do_close and abort
            # engine.dispose(). Matches the rollback branch's
            # classification. Programmer bugs (AttributeError /
            # TypeError) still propagate.
            try:
                await_only(self._connection.close())
            except (
                OperationalError,
                InterfaceError,
                DqliteConnectionError,
                OSError,
            ) as exc:
                peer = getattr(self._connection, "address", None)
                logger.debug(
                    "AsyncAdaptedConnection.close (id=%s, peer=%s): "
                    "close failed (%s); proceeding with teardown",
                    id(self),
                    peer,
                    type(exc).__name__,
                    exc_info=True,
                )

    def terminate(self) -> None:
        """Force-close the underlying connection without rollback.

        SQLAlchemy's async pool calls ``dialect.do_terminate(dbapi_conn)``
        (which defers to this method) when ``has_terminate = True`` and
        a connection must be forcibly reclaimed — typically during
        ``engine.dispose()`` under failure, or when a stuck rollback
        would otherwise block shutdown. Unlike ``close()`` we do NOT
        attempt rollback first: that's the whole point of terminate.
        """
        # ``has_terminate = True`` promises SA that this path never
        # blocks dispose; suppress transport-class failures so a flaky
        # close cannot abort forced reclaim.
        try:
            await_only(self._connection.close())
        except (
            OperationalError,
            InterfaceError,
            DqliteConnectionError,
            OSError,
        ) as exc:
            peer = getattr(self._connection, "address", None)
            logger.debug(
                "AsyncAdaptedConnection.terminate (id=%s, peer=%s): "
                "close failed (%s); teardown complete",
                id(self),
                peer,
                type(exc).__name__,
                exc_info=True,
            )


class DqliteDialect_aio(DqliteDialect):
    """Async SQLAlchemy dialect for dqlite.

    Use with SQLAlchemy's async engine:
        create_async_engine("dqlite+aio://host:port/database")
    """

    # Match the entry-point short name (``"dqlite.aio"`` in pyproject) so
    # ``dialect_description`` renders ``"dqlite+aio"`` — the exact form a
    # user writes into the URL (``dqlite+aio://host:port/db``) and the
    # form SA's error messages / logs / ``repr(engine)`` show. SA's own
    # aiosqlite reference does the same (EP ``sqlite.aiosqlite`` ↔
    # ``driver = "aiosqlite"`` ↔ URL ``sqlite+aiosqlite://``). The
    # prior value ``"dqlitedbapi_aio"`` produced a non-canonical
    # description string no user types, breaking log grep of the URL
    # shape.
    driver = "aio"
    is_async = True
    # MUST be redeclared here even though the base class already sets it to
    # True: SQLAlchemy reads this attribute via
    # ``self.__class__.__dict__.get("supports_statement_cache")`` (see
    # engine/default.py::_supports_statement_cache), which is a
    # single-class lookup, not an MRO lookup. If this line is removed,
    # statement caching is silently disabled on the async dialect and a
    # warning fires on every engine startup.
    supports_statement_cache = True

    # dqlite has no server-side cursor notion at the wire level — rows
    # arrive in frames that the client fully consumes before surfacing
    # them, and the adapter eagerly buffers into a deque. Pin False
    # locally so a future base-class default flip (e.g. AsyncDialect
    # defaulting True the way aiosqlite does) cannot silently route
    # through an SS-cursor code path the adapter does not implement.
    supports_server_side_cursors = False

    # SQLAlchemy's async pool gates its forced-disposal path on
    # ``has_terminate`` (see ``pool/base.py`` docs for
    # ``_ConnectionRecord.invalidate``). The reference aiosqlite
    # dialect sets this True; our ``AsyncAdaptedConnection`` now
    # provides a ``terminate()`` that skips rollback and closes
    # directly, so pin True locally to defend against an MRO flip
    # from the DefaultDialect default (``False``).
    has_terminate = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return AsyncAdaptedQueuePool

    def do_terminate(self, dbapi_connection: Any) -> None:
        """Integration point SQLAlchemy's async pool calls for forced
        disposal. Defers to ``AsyncAdaptedConnection.terminate()``,
        which closes without the usual pre-close rollback so a stuck
        rollback on a half-dead connection cannot block
        ``engine.dispose()``.
        """
        dbapi_connection.terminate()

    @classmethod
    def import_dbapi(cls) -> types.ModuleType:
        from dqlitedbapi import aio

        return aio

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Create and wrap an async connection.

        Eagerly establishes the TCP connection so errors surface at
        connect-time rather than on the first query. If that eager
        connect raises, the ``raw_conn`` object is already constructed
        and holds references to loop locks / partially-initialised
        state — without explicit cleanup it leaks until GC and can
        linger on the event loop it was bound to. Call ``close()`` on
        ``BaseException`` so cancellation (e.g. a parent
        ``asyncio.timeout()`` firing during connect) also cleans up,
        then re-raise unchanged. ``close()`` is documented as
        idempotent and safe to call even when no TCP connection
        landed, so the suppression around it is narrow
        (``Exception`` only; cancellation signals still propagate).
        """
        raw_conn = self.loaded_dbapi.connect(*cargs, **cparams)
        try:
            await_only(raw_conn.connect())
        except BaseException:
            with contextlib.suppress(Exception):
                await_only(raw_conn.close())
            raise
        return AsyncAdaptedConnection(raw_conn)

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the underlying driver-level connection."""
        return connection._connection
