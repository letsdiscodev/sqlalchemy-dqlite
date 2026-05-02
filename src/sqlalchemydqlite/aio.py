"""Async dqlite dialect for SQLAlchemy."""

import asyncio
import contextlib
import logging
import types
import weakref
from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, NoReturn

from sqlalchemy import pool
from sqlalchemy.engine import URL, AdaptedConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util import await_only
from sqlalchemy.util.concurrency import in_greenlet

from dqlitedbapi.exceptions import (
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitedbapi.types import _DescriptionTuple
from sqlalchemydqlite.base import _TRANSPORT_CLASS_EXCEPTIONS, DqliteDialect

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

__all__ = ["AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"]

# PEP 249 specifies ``cursor.description`` as a sequence of sequences —
# a ``list[tuple]`` is the canonical shape but a strict type alias of
# ``list`` would reject a dbapi cursor that returns a tuple-of-tuples
# (which sqlalchemy's own aiosqlite adapter accepts). Widen the outer
# alias to ``Sequence`` so the adapter passes through whatever the
# underlying cursor returns without copying. The inner 7-tuple shape is
# imported from the dbapi layer (single source of truth) so a future
# column (real display_size, etc.) propagates here automatically.
type _Description = Sequence[_DescriptionTuple] | None


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
        # Idempotent close: stdlib ``sqlite3.Cursor.close`` and the
        # dbapi ``Cursor.close`` / ``AsyncCursor.close`` siblings all
        # short-circuit on a second call. Without this gate, double-
        # close redundantly re-runs the scrub AND attempts to wrap
        # the already-proxied ``_adapt_connection`` /
        # ``_connection`` in a second ``weakref.proxy`` — which
        # raises ``TypeError`` against a ``weakproxy``. The
        # ``contextlib.suppress(TypeError)`` swallows the failure
        # so close stays idempotent in practice, but the
        # short-circuit makes the contract structural rather than
        # exception-suppression-based.
        if self._closed:
            return
        # Scrub the public read-attributes so post-close reads of
        # ``description`` / ``rowcount`` / ``lastrowid`` see a
        # consistent "no operation performed" surface. Plain
        # attributes have no _closed gating, so a caller (or SA's
        # Result layer) reading them after close would otherwise see
        # the last statement's values — composes badly with any
        # subsequent execute that should reset them.
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows.clear()
        self._closed = True
        # Drop the strong back-references to the parent adapter
        # and the inner dbapi connection so a closed cursor that
        # SA's pool-diagnostic ring / pytest fixture cache retains
        # does not pin the inner dbapi ``AsyncConnection`` — and
        # through it the client-layer state, registered
        # ``weakref.finalize``, and any frame-pinning
        # ``_invalidation_cause``. ``weakref.proxy`` preserves
        # forward attribute access while the inner is alive;
        # post-close calls on the proxy raise ``ReferenceError``
        # only if the inner has been GC'd, which is benign at
        # that point. Mirror discipline of dbapi-layer
        # ``Cursor.close`` / ``AsyncCursor.close``.
        with contextlib.suppress(TypeError):
            self._adapt_connection = weakref.proxy(self._adapt_connection)
        with contextlib.suppress(TypeError):
            self._connection = weakref.proxy(self._connection)

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> None:
        """Execute a single statement.

        ``parameters`` is typed as ``Sequence | Mapping | None`` to
        match the PEP 249 DB-API envelope that SQLAlchemy expects, but
        at runtime the underlying driver is ``paramstyle="qmark"`` and
        rejects mappings with ``ProgrammingError``. SA's own compiler
        always hands us a sequence, so the wider hint is documentary:
        it reflects the envelope the framework layers expect, not
        driver capability. Passing a ``dict`` directly will surface as
        ``ProgrammingError`` at the DBAPI cursor layer.
        """
        # Mirror the closed-cursor guard the other methods on this
        # class apply (fetch* / setinputsizes / scroll / etc.). Without
        # it, a stale execute on a closed adapter cursor silently
        # succeeds and the user only sees ``cursor is closed`` from
        # the first fetch — a confusing diagnostic that implies the
        # cursor was closed between execute and fetch.
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        # Clear buffered state FIRST so a CancelledError (or any other
        # exception) during execute/fetchall leaves the adapter in a
        # "no active result" state rather than carrying stale rows
        # from a previous execution.
        self.description = None
        self.rowcount = -1
        # Do NOT clear ``lastrowid`` here. stdlib ``sqlite3.Cursor.lastrowid``
        # and the dbapi-layer ``Cursor._execute_async`` both honour the
        # sticky-INSERT contract: ``lastrowid`` survives a subsequent
        # UPDATE / DELETE / DDL / SELECT on the same cursor and is
        # cleared only on ``close()``. The async adapter opens a fresh
        # underlying ``AsyncCursor`` per ``execute()``, so we cannot
        # rely on the dbapi cursor's stickiness directly — instead we
        # preserve the adapter's prior value across non-INSERT
        # executes by writing only when the underlying cursor reports
        # a non-None value (i.e. an INSERT/REPLACE actually ran).
        self._rows.clear()

        # Hoist ``self._connection.cursor()`` inside the try so a
        # synchronous raise from cursor() (closed connection,
        # cross-loop ProgrammingError) routes through
        # ``_handle_exception`` and gets normalized just like a
        # cursor.execute raise. Initialize ``cursor`` to None so the
        # finally close path skips when cursor() failed.
        cursor: AsyncCursor | None = None
        try:
            try:
                cursor = self._connection.cursor()
                if parameters is not None:
                    # SA's envelope passes ``Sequence | Mapping`` here; the
                    # underlying qmark-paramstyle dbapi rejects mappings at
                    # runtime with a clear DataError. Match the typing
                    # widening upstream by ignoring the assignment-time
                    # narrowness gap.
                    await_only(cursor.execute(operation, parameters))  # type: ignore[arg-type]
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
                    if cursor.lastrowid is not None:
                        self.lastrowid = cursor.lastrowid
                else:
                    if cursor.lastrowid is not None:
                        self.lastrowid = cursor.lastrowid
                    self.rowcount = cursor.rowcount
            except BaseException as error:
                # Route every cursor-level error through the connection's
                # _handle_exception hook so a single override remaps
                # driver-layer quirks (loop-mismatch RuntimeError,
                # client-layer subclass shape, etc.) once instead of
                # at every execute call site. Mirrors SA's reference
                # AsyncAdapt_aiosqlite_cursor wrap-all pattern.
                self._adapt_connection._handle_exception(error)
        finally:
            # Only close if cursor was successfully constructed —
            # cursor() may have raised inside the try.
            if cursor is not None:
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
                try:
                    await_only(cursor.close())
                except (Exception, asyncio.CancelledError) as exc:
                    # DEBUG-log the suppressed close failure so a
                    # flapping leader (close fails repeatedly post-
                    # execute) is observable in logs. Mirrors the
                    # discipline applied to AsyncAdaptedConnection
                    # close()/terminate() and to the dialect-side
                    # do_ping close arm.
                    peer = getattr(self._adapt_connection._connection, "address", None)
                    logger.debug(
                        "AsyncAdaptedCursor.execute (id=%s, peer=%s): "
                        "underlying cursor close raised %s; suppressed",
                        id(self),
                        peer,
                        type(exc).__name__,
                        exc_info=True,
                    )

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        """Execute many statements.

        As with ``execute``, mapping parameters are rejected by the
        underlying qmark-paramstyle driver at runtime. The wider hint
        matches SA's envelope; see ``execute`` for the rationale.
        """
        # Mirror the closed-cursor guard the other methods on this
        # class apply; see ``execute`` for the rationale.
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        # Clear state up-front so cancellation mid-call doesn't leak
        # a previous execution's buffered rows. ``lastrowid`` is NOT
        # cleared here (sticky-INSERT contract — see ``execute`` for
        # rationale).
        self.description = None
        self.rowcount = -1
        self._rows.clear()

        # Hoist cursor() inside the try, mirroring execute() — a
        # synchronous raise (closed conn, cross-loop ProgrammingError)
        # must route through _handle_exception too.
        cursor: AsyncCursor | None = None
        try:
            try:
                cursor = self._connection.cursor()
                # See execute() — mappings are rejected at runtime by the
                # qmark dbapi; match the SA-envelope widening here too.
                await_only(cursor.executemany(operation, seq_of_parameters))  # type: ignore[arg-type]
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
                    if cursor.lastrowid is not None:
                        self.lastrowid = cursor.lastrowid
                else:
                    if cursor.lastrowid is not None:
                        self.lastrowid = cursor.lastrowid
                    self.rowcount = cursor.rowcount
            except BaseException as error:
                # Same routing as ``execute``: errors flow through the
                # connection's _handle_exception hook for centralized
                # remapping.
                self._adapt_connection._handle_exception(error)
        finally:
            if cursor is not None:
                # Same narrow suppression as ``execute``'s finally block
                # above — see the rationale there. Keeps KI / SystemExit
                # propagating while still covering greenlet cancellation.
                try:
                    await_only(cursor.close())
                except (Exception, asyncio.CancelledError) as exc:
                    peer = getattr(self._adapt_connection._connection, "address", None)
                    logger.debug(
                        "AsyncAdaptedCursor.executemany (id=%s, peer=%s): "
                        "underlying cursor close raised %s; suppressed",
                        id(self),
                        peer,
                        type(exc).__name__,
                        exc_info=True,
                    )

    def fetchone(self) -> Any | None:
        # Narrow from ``Any`` so callers understand None is a legitimate
        # return on exhaustion (PEP 249 contract, mirroring the
        # dqlitedbapi sync / async cursors that already type this as
        # ``tuple[Any, ...] | None``). Runtime behaviour unchanged.
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size: int | None = None) -> Sequence[Any]:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        if size is None:
            size = self.arraysize
        if size < 0:
            raise ProgrammingError(f"fetchmany size must be non-negative, got {size}")
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> Sequence[Any]:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        retval = list(self._rows)
        self._rows.clear()
        return retval

    def setinputsizes(self, *args: Any) -> None:
        # PEP 249: called before execute*() to hint bind-parameter sizes.
        # dqlite's wire encoder does not use per-parameter sizing hints,
        # so the implementation is a no-op on an open cursor — but the
        # closed-cursor case must raise to match the underlying
        # AsyncCursor's behaviour and to keep ``is_disconnect``'s
        # narrow "cursor is closed" InterfaceError branch reachable
        # through the adapter.
        #
        # Accept BOTH PEP 249's single-sequence shape
        # (``cur.setinputsizes([size_a, size_b])``) AND SA's connector-
        # reference variadic shape
        # (``cur.setinputsizes(size_a, size_b)``,
        # see sqlalchemy.connectors.asyncio:
        # ``def setinputsizes(self, *inputsizes)``). Without the
        # variadic accept-arm a SA-internal call passing positional
        # sizes would raise ``TypeError`` on the unexpected count;
        # without the single-sequence arm a PEP 249 caller would still
        # work today (the body is a no-op) but would silently drop
        # the type information if the body ever stops being a no-op.
        # _ = args  # body is a no-op; sizes/inputsizes are inspected
        # downstream only if a future implementation honours the hint.
        del args
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")

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
    def callproc(self, procname: str, parameters: Sequence[Any] | None = None) -> NoReturn:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> NoReturn:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> NoReturn:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        # PEP 249 §6.1.1 enumerates ``mode`` ∈ {"relative", "absolute"};
        # validate before NotSupportedError so a caller typo surfaces
        # as a caller-side bug. ProgrammingError stays in dbapi.Error.
        if mode not in ("relative", "absolute"):
            from dqlitedbapi.exceptions import ProgrammingError

            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
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

    def __enter__(self) -> "AsyncAdaptedCursor":
        # SA's reference connector cursor and aiosqlite cursor both
        # support the context-manager protocol so callers can
        # ``with conn.execute(...) as cur:``. The body simply yields
        # ``self``; ``__exit__`` closes the cursor.
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        # PEP 343 ``__exit__`` signature. The body always closes and
        # never suppresses, so ``-> None`` is the correct return; a
        # truthy return would silently swallow caller exceptions.
        self.close()

    def __reduce__(self) -> NoReturn:
        # The class does NOT hold a long-lived ``AsyncCursor`` — each
        # ``execute`` / ``executemany`` opens a fresh dbapi cursor in
        # a finally-close block (greenlet-eager-fetch). The reject
        # fires via the strong back-references to ``_adapt_connection``
        # and ``_connection`` (a live ``AsyncConnection`` bound to an
        # asyncio loop). Surface a clear driver-level TypeError
        # naming the SA-adapter class specifically — without this,
        # the default pickle walk eventually trips on the
        # ``AsyncConnection``'s loop-bound state with a
        # wrong-layer diagnostic.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — back-"
            f"references a loop-bound dbapi AsyncConnection holding a "
            f"live socket and asyncio.Lock; reconstruct from the "
            f"engine in the target process instead."
        )


class AsyncAdaptedConnection(AdaptedConnection):
    """Adapts an AsyncConnection for SQLAlchemy's greenlet-based async engine.

    Provides sync-looking methods that internally use await_only() to
    bridge to the underlying async connection within SQLAlchemy's
    greenlet context.

    Cursor lifecycle: ``AsyncAdaptedCursor`` does NOT hold a long-lived
    dbapi cursor — each ``execute`` / ``executemany`` call opens a
    fresh ``dqlitedbapi.aio.AsyncCursor`` and closes it in a finally
    block. As a result the adapter does not need to track cursors for
    cascade-close: closing the adapter connection closes the
    underlying ``AsyncConnection`` (which has its own cursor cascade
    for any long-lived dbapi cursors). A stale adapter cursor used
    after the parent adapter connection is closed will surface
    ``InterfaceError`` from the next execute attempt's
    ``self._connection.cursor()`` call. Mirrors SA's reference
    ``AsyncAdapt_aiosqlite_connection`` which also does not track
    adapter cursors.
    """

    # Parent ``sqlalchemy.engine.interfaces.AdaptedConnection`` declares
    # ``__slots__ = ("_connection",)``; without our own slots declaration
    # each instance gets a ``__dict__`` and defeats the parent's memory
    # optimization (SA's own ``AsyncAdapt_aiosqlite_connection`` follows
    # the same pattern). We add no new instance attributes, so an empty
    # slots tuple is correct.
    __slots__ = ()

    # SA convention (asyncpg.py:714, aiosqlite.py:257,
    # connectors/asyncio.py:338): expose ``await_`` as a staticmethod on
    # the connection class. External instrumentation (Sentry / Datadog
    # async-driver wrappers, SQLModel, sqlalchemy-utils) introspects
    # ``dbapi_connection.await_`` to coalesce sync/async hops without
    # re-running greenlet detection. The staticmethod surface is also
    # the documented hook for a hypothetical ``AsyncAdaptFallback_*``
    # variant: flip one line to ``staticmethod(await_fallback)`` and
    # propagate.
    #
    # Internal call sites in this module continue to call ``await_only``
    # directly from module scope — keeping the existing test fixtures
    # that ``monkeypatch.setattr(aio_module, "await_only", ...)`` for
    # behavioural stubs working unchanged. The staticmethod is purely a
    # documented public surface for third-party callers and SA's own
    # extension points.
    await_ = staticmethod(await_only)

    def __init__(self, connection: "AsyncConnection") -> None:
        # ``_connection`` is the concrete ``dqlitedbapi.aio.AsyncConnection``
        # this adapter wraps; SQLAlchemy's parent ``AdaptedConnection``
        # declares the attribute with a wider Protocol type, so we keep
        # the store on ``Any`` and rely on the annotation here to document
        # the intended input shape.
        self._connection: Any = connection

    def __reduce__(self) -> NoReturn:
        # Wraps a live ``AsyncConnection`` (loop-bound, holds a live
        # socket and asyncio.Lock). Surface a clear driver-level
        # TypeError naming the SA-adapter class specifically —
        # without this, the underlying ``AsyncConnection.__reduce__``
        # raises a TypeError naming the dbapi class, which is a
        # wrong-layer diagnostic for SA users.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — wraps a "
            f"loop-bound dbapi AsyncConnection holding a live socket "
            f"and asyncio.Lock; reconstruct from the engine in the "
            f"target process instead."
        )

    def cursor(self, server_side: bool = False) -> AsyncAdaptedCursor:
        # Match the SA connector reference signature
        # (``sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection.cursor``)
        # which takes ``server_side: bool = False``. The dialect pins
        # ``supports_server_side_cursors=False`` so SA itself never
        # passes ``server_side=True`` here, but third-party callers
        # and future SA paths may; raise ``NotImplementedError``
        # explicitly so misuse surfaces clearly rather than failing
        # opaquely on an unexpected kwarg.
        if server_side:
            raise NotImplementedError(
                "Server-side cursors are not supported by the dqlite dialect; "
                "supports_server_side_cursors is pinned to False."
            )
        # Closed-state guard: ``close()`` replaces ``self._connection``
        # with ``weakref.proxy(...)``. Returning a fresh
        # ``AsyncAdaptedCursor`` over a proxy that may have been GC'd
        # would defer the diagnostic to the first ``execute()``, which
        # then surfaces either ``InterfaceError("Connection is closed
        # ...")`` (proxied alive-but-closed) or — worse —
        # ``ReferenceError`` (proxied GC'd) that is NOT a
        # ``dbapi.Error`` subclass and escapes SA's
        # ``_handle_dbapi_exception`` classifier. Detect the post-close
        # state via the proxy type check and raise ``InterfaceError``
        # up front, matching the dbapi-layer ``AsyncConnection.cursor``
        # discipline.
        if isinstance(self._connection, weakref.ProxyTypes):
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        return AsyncAdaptedCursor(self)

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncAdaptedCursor:
        """SA-reference parity: open a cursor, run ``execute``, return
        the cursor. SA's reference ``connectors/asyncio.py`` exposes
        this and SA-internal code paths (e.g.,
        ``dialects/sqlite/provision.py``) call
        ``dbapi_connection.execute(...)`` directly. Without this
        method the call hits ``AttributeError`` on the dqlite adapter.

        On synchronous failure of ``cur.execute(...)`` (a closed
        connection, cross-loop misuse, etc.) close the freshly-opened
        cursor before re-raising so the caller's exception path
        doesn't leak an unowned cursor with loop-bound state. SA's
        reference connector follows the same try/close/raise
        discipline.
        """
        cur = self.cursor()
        try:
            if parameters is None:
                cur.execute(operation)
            else:
                cur.execute(operation, parameters)
        except BaseException:
            with contextlib.suppress(Exception):
                cur.close()
            raise
        return cur

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

    @property
    def autocommit(self) -> bool:
        """Report ``False``: SA's transaction model is in effect at this layer.

        The underlying dbapi ``Connection.autocommit`` is ``True`` —
        the dqlite wire protocol is autocommit-by-default and every
        statement commits at the server unless the caller issued an
        explicit ``BEGIN``. The SA adapter deliberately reports
        ``False`` here because SA wraps the connection with explicit
        ``BEGIN`` / ``COMMIT`` control via the dialect, taking the
        wire layer out of autocommit mode for the duration of the
        SA-managed transaction. Both layers' values are accurate for
        their respective layer; they are not in conflict.

        Parity with SA's reference ``AsyncAdapt_aiosqlite_connection``,
        which exposes ``autocommit`` as a read/write property. SA
        characteristic code and some third-party middleware probe
        ``getattr(dbapi_conn, "autocommit", None)`` — without the
        property those probes see ``None`` and may log misleading
        "autocommit unknown" diagnostics.
        """
        return False

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Reject attempts to enable AUTOCOMMIT mode at the SA layer;
        accept ``False`` as a no-op.

        SA's engine flow short-circuits ``set_isolation_level`` to
        reject ``"AUTOCOMMIT"`` before reaching the dialect, but a
        direct ``conn.autocommit = True`` on the adapter would bypass
        that guard. Fail fast with the same educational message the
        dialect emits for ``isolation_level="AUTOCOMMIT"``. The
        underlying wire is autocommit-by-default; what's rejected
        here is SA's AUTOCOMMIT *isolation level*, which would
        require the dialect to skip BEGIN/COMMIT wrapping — not
        compatible with how the adapter manages the dqlite
        connection.
        """
        if value:
            from sqlalchemy.exc import ArgumentError

            from sqlalchemydqlite.base import _AUTOCOMMIT_REJECTION_MSG

            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        # value is False → already the effective mode, no-op.

    def _handle_exception(self, error: BaseException) -> NoReturn:
        """Adapter-level exception normalisation hook.

        Matches the ``AsyncAdapt_aiosqlite_connection._handle_exception``
        extension point in SA's reference dialect. Centralises the
        remap of driver-layer quirks so commit/rollback/execute /
        executemany do not each re-implement the same translation.

        Concrete remaps:

        * ``RuntimeError`` from ``await_only`` whose message contains
          ``"different loop"`` (canonical Python wording: ``"got Future
          ... attached to a different loop"``) — surfaces when an
          ``AsyncConnection`` is reused across two event loops (e.g.,
          ``asyncio.run()`` per call). The bare ``RuntimeError`` would
          not be classified by SA (``isinstance(e, dbapi.Error)`` gates
          ``is_disconnect``), so the pool would not invalidate the slot
          and the next checkout would hit the same fault.
        * ``ProgrammingError`` from ``dqlitedbapi.AsyncConnection`` whose
          message contains ``"different event loop"`` (full phrase) —
          surfaces from ``_ensure_locks`` / ``cursor()`` on the same
          cross-loop reuse pattern. ``is_disconnect`` deliberately does
          not classify ProgrammingError as a disconnect on real-query
          paths (programmer-bug shapes must stay visible), so without
          a remap the pool slot would survive the cross-loop fault.

        Both shapes route through one substring scan over the two
        canonical wordings — Python's ``"different loop"`` (which
        ``"attached to a different loop"`` already contains) and the
        dbapi's ``"different event loop"`` (a distinct phrase, NOT a
        superstring of the first since ``"event "`` sits between
        ``"different"`` and ``"loop"``). Re-raise as
        ``dbapi.OperationalError`` (with the substring preserved) so
        the dialect's substring fallback classifies it as a disconnect.
        """
        if isinstance(error, (RuntimeError, ProgrammingError)):
            msg = str(error)
            # Both substrings are needed: ``"different loop"`` matches
            # Python's ``"attached to a different loop"`` and any
            # variant that uses the bare phrase, while
            # ``"different event loop"`` matches dqlitedbapi's distinct
            # wording. The redundant ``or "attached to a different
            # loop"`` clause from the older arm was dropped — that
            # phrase strictly contains ``"different loop"`` so the
            # first check already matches it.
            if "different loop" in msg or "different event loop" in msg:
                raise OperationalError(f"event-loop mismatch: {msg}", code=None) from error
            # ``RuntimeError("Event loop is closed")`` reaches us via
            # ``commit`` / ``rollback`` / ``execute`` / ``executemany``
            # when the asyncio loop has been torn down between
            # checkout and the operation (per-call ``asyncio.run()``
            # patterns). Without a remap it leaks as a bare
            # RuntimeError past SA's ``is_disconnect`` classifier
            # (which is gated on ``DatabaseError``) and the broken
            # slot survives. Treat as a transport-class disconnect
            # so the pool invalidates and the next checkout gets a
            # fresh connection. Symmetric with the
            # ``"Event loop is closed"`` substring already in the
            # base.py ``_dqlite_disconnect_messages`` tuple.
            if "Event loop is closed" in msg:
                raise OperationalError(f"event loop closed: {msg}", code=None) from error
        raise error

    def commit(self) -> None:
        try:
            await_only(self._connection.commit())
        except BaseException as error:
            self._handle_exception(error)

    def rollback(self) -> None:
        try:
            await_only(self._connection.rollback())
        except BaseException as error:
            self._handle_exception(error)

    def close(self) -> None:
        # Preflight on ``in_greenlet()`` matches SA's reference adapter
        # idiom (``connectors/asyncio.py:217-220, 392-415``). Outside a
        # greenlet (GC sweep / atexit / non-greenlet finalize), skip
        # both the rollback and the async close entirely and reap the
        # writer synchronously. ``await_only`` would otherwise allocate
        # a ``MissingGreenlet`` exception with full traceback only to be
        # caught and absorbed; the preflight avoids that throw.
        if not in_greenlet():
            self._force_close_transport()
            return

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
            except _TRANSPORT_CLASS_EXCEPTIONS as exc:
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
            except RuntimeError as exc:
                # Route loop-mismatch RuntimeError through the same
                # remap as commit/rollback/execute/executemany so SA's
                # is_disconnect classifier (which is gated on
                # DatabaseError) sees an OperationalError instead of
                # a bare RuntimeError. Without this, cross-loop
                # close() would propagate an un-classified RuntimeError
                # past engine.dispose().
                msg = str(exc)
                if "different loop" in msg or "different event loop" in msg:
                    self._handle_exception(exc)
                # ``RuntimeError("Event loop is closed")`` lands here
                # during ``engine.dispose()`` after a per-call
                # ``asyncio.run()`` finished and tore the loop down —
                # symmetric with the close arm below. The
                # ``has_terminate=True`` dialect promise says
                # close()/dispose must not propagate failures from
                # this path; debug-log and continue to the close arm
                # in the finally (which will itself raise the same
                # RuntimeError but absorb it via the matching arm).
                # The debug log preserves the traceback for triage.
                if "Event loop is closed" in msg:
                    peer = getattr(self._connection, "address", None)
                    logger.debug(
                        "AsyncAdaptedConnection.close (id=%s, peer=%s): "
                        "rollback raised RuntimeError (%s); proceeding to close",
                        id(self),
                        peer,
                        type(exc).__name__,
                        exc_info=True,
                    )
                    return
                # Other RuntimeErrors (programmer bugs) propagate.
                raise
            # The non-greenlet path is handled by the ``in_greenlet()``
            # preflight at the top of ``close()``; ``MissingGreenlet``
            # cannot land here.
            # ``CancelledError`` from the rollback await is allowed to
            # propagate so the cancellation signal is preserved — the
            # finally below still runs close(), and the close arm's
            # CancelledError catch routes through the sync force-close
            # fallback before re-raising. Suppressing here would
            # convert a still-active cancel into a clean return,
            # contradicting asyncio's "cancellation propagates"
            # contract; the prior test
            # ``test_close_runs_close_after_rollback_raise.py`` pins
            # that contract.
        finally:
            # Narrow the close-time exception set to transport-class
            # failures. A transient OSError / DqliteConnectionError
            # mid-close must not escape do_close and abort
            # engine.dispose(). Matches the rollback branch's
            # classification. Programmer bugs (AttributeError /
            # TypeError) still propagate.
            try:
                await_only(self._connection.close())
            except _TRANSPORT_CLASS_EXCEPTIONS as exc:
                peer = getattr(self._connection, "address", None)
                logger.debug(
                    "AsyncAdaptedConnection.close (id=%s, peer=%s): "
                    "close failed (%s); proceeding with teardown",
                    id(self),
                    peer,
                    type(exc).__name__,
                    exc_info=True,
                )
            except RuntimeError as exc:
                # ``RuntimeError("Event loop is closed")`` /
                # ``RuntimeError("...attached to a different loop")``
                # land here during ``engine.dispose()`` after a per-
                # call ``asyncio.run()`` finished and tore the loop
                # down. The async machinery cannot run; reap the
                # writer synchronously so the transport doesn't leak.
                # ``has_terminate=True`` (the dialect-level promise)
                # means close()/dispose must not propagate failures
                # from this path; the debug log preserves the
                # traceback for triage.
                peer = getattr(self._connection, "address", None)
                logger.debug(
                    "AsyncAdaptedConnection.close (id=%s, peer=%s): "
                    "close raised RuntimeError (%s); reaped transport "
                    "synchronously",
                    id(self),
                    peer,
                    type(exc).__name__,
                    exc_info=True,
                )
                self._force_close_transport()
            except asyncio.CancelledError:
                # Cancel landing on the close await (canonical trigger:
                # an outer ``asyncio.timeout`` mid-``engine.dispose()``
                # under SIGTERM-with-budget). Run the sync transport
                # fallback so the writer is closed even though the
                # async machinery was interrupted, then re-raise so
                # the cancel still propagates to the caller.
                self._force_close_transport()
                raise
        # Drop the strong back-reference to the inner dbapi
        # ``AsyncConnection`` so a closed adapter retained by SA's
        # pool diagnostics / pytest session-fixture cache does
        # not pin the inner conn — and through it the client-
        # layer state, registered ``weakref.finalize``, and any
        # frame-pinning ``_invalidation_cause``. ``weakref.proxy``
        # preserves SA's expected API surface (calls forward to
        # the inner while it is alive) — only after the inner is
        # genuinely GC'd does ``ReferenceError`` surface, which
        # is benign post-close. SA's reference adapter keeps the
        # strong ref; this is dqlite-specific lifecycle
        # discipline matching the dbapi layer's
        # ``AsyncConnection.close``.
        with contextlib.suppress(TypeError):
            self._connection = weakref.proxy(self._connection)

    def _force_close_transport(self) -> None:
        """Best-effort synchronous teardown of the underlying transport.

        Bypasses the async ``DqliteConnection.close`` machinery
        (which requires an event loop / greenlet context) and closes
        the writer transport directly. The reader half is closed by
        the OS as a side effect of the writer close. Used when SA's
        finalize path runs outside a greenlet (e.g., GC sweep), where
        ``await_only`` would raise ``MissingGreenlet`` and the SA pool
        would silently absorb it.

        Delegates to the dbapi connection's public
        ``force_close_transport`` hook so the access boundary stays
        on a single supported method instead of walking three layers
        of private attributes.

        Idempotent. Never raises — a missing hook (older dbapi
        version) or a writer.close() failure is silently absorbed.
        """
        peer = getattr(self._connection, "address", None)
        hook = getattr(self._connection, "force_close_transport", None)
        try:
            if hook is None:
                # Older dbapi without the force-close hook; nothing
                # we can do synchronously. Log so the audit trail
                # records the no-op rather than silently lying about
                # delegating.
                logger.debug(
                    "AsyncAdaptedConnection._force_close_transport (id=%s, peer=%s): "
                    "dbapi connection has no force_close_transport hook; "
                    "transport teardown skipped",
                    id(self),
                    peer,
                )
                return
            hook()
            logger.debug(
                "AsyncAdaptedConnection._force_close_transport (id=%s, peer=%s): "
                "delegated to dbapi force_close_transport (sync fallback)",
                id(self),
                peer,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "AsyncAdaptedConnection._force_close_transport (id=%s, peer=%s): "
                "best-effort sync close raised (%s); ignoring",
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
        # Preflight on ``in_greenlet()`` — see ``close()`` for
        # rationale. Non-greenlet finalize paths reap the writer
        # synchronously without paying the ``MissingGreenlet``
        # exception-allocation cost.
        if not in_greenlet():
            self._force_close_transport()
            return

        # ``has_terminate = True`` promises SA that this path never
        # blocks dispose; suppress transport-class failures so a flaky
        # close cannot abort forced reclaim. Cancel landing during
        # the close is handled by the explicit CancelledError catch
        # below which calls ``_force_close_transport`` synchronously
        # (the writer.close() bypasses the cancel-poisoned async
        # machinery). ``asyncio.shield`` cannot be applied here — the
        # await runs through ``await_only`` from a sync greenlet
        # context where ``shield``'s loop-binding semantics don't
        # apply, and the explicit catch already covers the same case.
        try:
            await_only(self._connection.close())
        except _TRANSPORT_CLASS_EXCEPTIONS as exc:
            peer = getattr(self._connection, "address", None)
            logger.debug(
                "AsyncAdaptedConnection.terminate (id=%s, peer=%s): "
                "close failed (%s); teardown complete",
                id(self),
                peer,
                type(exc).__name__,
                exc_info=True,
            )
        except RuntimeError as exc:
            # Defunct-loop close during ``engine.dispose()``: an
            # ``asyncio.run()`` per-call pattern tears the loop down,
            # then SA's pool finalizer calls ``terminate()`` and the
            # async machinery raises ``RuntimeError("Event loop is
            # closed")``. ``has_terminate=True`` promises SA that
            # dispose never propagates failures from this path; reap
            # the writer synchronously and stay quiet (DEBUG only).
            peer = getattr(self._connection, "address", None)
            logger.debug(
                "AsyncAdaptedConnection.terminate (id=%s, peer=%s): "
                "close raised RuntimeError (%s); reaped transport "
                "synchronously",
                id(self),
                peer,
                type(exc).__name__,
                exc_info=True,
            )
            self._force_close_transport()
        except asyncio.CancelledError:
            # See close()'s sibling catch — outer cancel during a
            # forced reclaim must still close the writer transport
            # synchronously before propagating, otherwise SA's
            # ``has_terminate=True`` promise (the pool can always
            # reclaim a slot) breaks under SIGTERM-with-budget
            # shutdown.
            self._force_close_transport()
            raise


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
        # Returns ``dqlitedbapi.aio``, NOT the top-level ``dqlitedbapi``
        # module that the sync dialect imports (see
        # ``DqliteDialect.import_dbapi`` in base.py). The async dialect
        # drives ``AsyncConnection`` / ``AsyncCursor`` from the ``aio``
        # submodule; aligning the two would silently break the async
        # path. Asymmetry is deliberate.
        from dqlitedbapi import aio

        return aio

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Create and wrap an async connection.

        Validate ``cparams`` against ``_CONNECT_KWARG_ALLOWED`` before
        forwarding so a typo in ``create_engine(connect_args={...})``
        raises ``ArgumentError`` with the same diagnostic class the
        URL query path emits at engine construction (mirrors the
        sync ``DqliteDialect.connect``).

        Eagerly establishes the TCP connection so errors surface at
        connect-time rather than on the first query. If that eager
        connect raises, the ``raw_conn`` object is already constructed
        and holds references to loop locks / partially-initialised
        state — without explicit cleanup it leaks until GC and can
        linger on the event loop it was bound to. The cleanup uses
        the SA-adapter's ``terminate()`` shape (force-close, no
        rollback) wrapped in a temporary ``AsyncAdaptedConnection``:
        attempting a graceful ``close()`` on a connection whose
        handshake never completed is meaningless and can re-raise
        ``RuntimeError("Event loop is closed")`` from a per-call
        ``asyncio.run()`` torn down by the failed connect, replacing
        the original error. ``terminate()`` short-circuits to
        ``_force_close_transport()`` outside a greenlet and shields
        the close await otherwise — both branches are safe under
        cancel and have no rollback path to crash on.
        Narrow the cleanup-suppress to ``(Exception,
        asyncio.CancelledError)`` so KeyboardInterrupt and SystemExit
        propagate through cleanup — matching the discipline applied
        elsewhere on dispose paths. ``terminate()`` itself runs the
        synchronous transport reap regardless of whether the suppress
        absorbs.
        """
        self._validate_connect_kwargs(cparams)
        raw_conn = self.loaded_dbapi.connect(*cargs, **cparams)
        try:
            await_only(raw_conn.connect())
        except BaseException:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                AsyncAdaptedConnection(raw_conn).terminate()
            raise
        return AsyncAdaptedConnection(raw_conn)

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the underlying driver-level connection."""
        return connection._connection
