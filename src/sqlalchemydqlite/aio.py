"""Async dqlite dialect for SQLAlchemy."""

import asyncio
import contextlib
import logging
import types
import weakref
from collections import deque
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, NoReturn, Self

from sqlalchemy import pool
from sqlalchemy.engine import URL, AdaptedConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util import await_only
from sqlalchemy.util.concurrency import in_greenlet

from dqlitedbapi import (
    DescriptionTuple,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemydqlite.base import (
    _TRANSPORT_CLASS_EXCEPTIONS,
    DqliteDialect,
    _log_safe_peer,
    _walk_cause_chain,
)

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
type _Description = Sequence[DescriptionTuple] | None


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

    **Divergence from SA reference connector**: SA's
    ``AsyncAdapt_dbapi_cursor``
    (``sqlalchemy/connectors/asyncio.py:166-186``) exposes
    ``description`` / ``rowcount`` / ``lastrowid`` as ``@property``
    that delegate to the underlying long-lived dbapi cursor. This
    adapter stores them as plain attributes (in ``__slots__``)
    because each ``execute()`` / ``executemany()`` opens and closes
    a fresh ``dqlitedbapi.aio.AsyncCursor`` inside a finally block —
    there is no live underlying cursor to delegate to after the call
    returns. Plain attributes carry the metadata through to the next
    execute. Consequence: SA's reference ``_async_soft_close()``
    memoizes ``description`` into ``_soft_closed_memoized`` for
    post-soft-close reads via the property layer; this adapter's
    ``_async_soft_close()`` is a no-op because the underlying cursor
    is already closed. If a future SA release adds new behaviour
    that hooks the property descriptor (e.g. a soft-close memoization
    layer above ``@property``), this adapter must be reshaped to
    keep the underlying cursor alive across the execute boundary,
    then converted to ``@property`` delegation.
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
        self._rows: deque[tuple[Any, ...]] = deque()
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
        """PEP 249 §6.1.2 setter for the per-``fetchmany`` batch size.

        **Semantic note (deque-only governance)**: this controls the
        deque-pop batch size of the adapter's pre-drained ``_rows``
        buffer. dqlite's wire protocol delivers the entire result set
        up-front in a single RTT, so ``arraysize`` does NOT influence
        wire-layer prefetch — that's an aiosqlite / pysqlite pattern
        not applicable here. The underlying ``AsyncCursor.arraysize``
        has the same deque-only semantic (the dbapi layer also
        eagerly buffers ``_rows`` from the wire response); both
        defaults are ``1`` per PEP 249. Done finding 602 covers the
        adapter-to-underlying propagation gap.

        Validation rejects ``bool``, non-int, and ``< 1`` so the
        ``arraysize=0`` / ``arraysize=-1`` footguns are caught at the
        assignment rather than silently turning every ``fetchmany``
        call into ``[]`` (which makes ``while batch :=
        cursor.fetchmany(): ...`` skip the entire result set).
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
        parameters: Sequence[Any] | None = None,
    ) -> None:
        """Execute a single statement.

        ``parameters`` is narrowed to ``Sequence | None`` because the
        underlying dbapi is ``paramstyle="qmark"`` and rejects mappings
        at runtime with ``ProgrammingError``. SA's own compiler always
        hands a sequence to qmark dialects — the wider PEP 249 envelope
        is unreachable through this driver, so the static type matches
        the runtime contract. Mapping passes here would have raised at
        the DBAPI cursor layer regardless; the narrower hint surfaces
        the rejection at typecheck time instead of the first execute.
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
                    await_only(cursor.execute(operation, parameters))
                else:
                    await_only(cursor.execute(operation))

                if cursor.description:
                    # Atomic-on-success: capture EVERYTHING into locals
                    # first, run the destructive ``drain_rows`` last,
                    # then assign all public fields together. If
                    # ``drain_rows`` raises (e.g. CancelledError from an
                    # outer timeout, server fault mid-stream), the
                    # adapter stays at the no-result baseline rather
                    # than leaving ``description`` populated with empty
                    # rows — which SA's Result layer would treat as an
                    # empty result set, indistinguishable from "execute
                    # succeeded but fetched no rows".
                    #
                    # ``drain_rows`` (sync, ownership-transfer): hand
                    # the dbapi cursor's row buffer to the adapter
                    # without an intermediate ``fetchall()`` copy. Cuts
                    # peak memory in half for INSERT...RETURNING
                    # insertmanyvalues at high row counts (100k+) where
                    # the cursor's list AND the adapter's deque would
                    # otherwise both be alive until the cursor is
                    # closed in the finally.
                    description = cursor.description
                    rowcount = cursor.rowcount
                    lastrowid = cursor.lastrowid
                    drained = deque(cursor.drain_rows())
                    self.description = description
                    self._rows = drained
                    # Mirror the DML branch: rowcount / lastrowid are set
                    # by the underlying cursor on the RETURNING path too.
                    # SA's Result layer reads both through the adapter,
                    # so leaving rowcount at -1 would silently collapse
                    # "N rows returned" into "not determinable".
                    self.rowcount = rowcount
                    if lastrowid is not None:
                        self.lastrowid = lastrowid
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
                    # ``AsyncCursor.close`` is sync; no ``await_only``
                    # bridge needed.
                    cursor.close()
                except (Exception, asyncio.CancelledError) as exc:
                    # DEBUG-log the suppressed close failure so a
                    # flapping leader (close fails repeatedly post-
                    # execute) is observable in logs. Mirrors the
                    # discipline applied to AsyncAdaptedConnection
                    # close()/terminate() and to the dialect-side
                    # do_ping close arm.
                    peer = _log_safe_peer(self._adapt_connection._connection)
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
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> None:
        """Execute many statements.

        As with ``execute``, ``parameters`` is narrowed to ``Sequence``
        per the qmark-only dbapi contract; mappings are rejected at the
        DBAPI cursor layer at runtime, and SA's compiler always hands
        a sequence to qmark dialects.
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
                await_only(cursor.executemany(operation, seq_of_parameters))
                # Mirror execute()'s post-call pattern: if the statement had
                # a RETURNING clause, the underlying cursor accumulates rows
                # across parameter sets and sets a description. Skipping the
                # description/rows capture silently loses every returned row
                # when SQLAlchemy's insertmanyvalues + RETURNING path is
                # driven through the async engine.
                if cursor.description:
                    # Same drain-rows ownership-transfer pattern as
                    # ``execute`` — atomic-on-success: capture metadata
                    # AND drain into locals first, then commit. A
                    # raise from ``drain_rows`` leaves the adapter at
                    # the no-result baseline, NOT half-populated.
                    description = cursor.description
                    rowcount = cursor.rowcount
                    lastrowid = cursor.lastrowid
                    drained = deque(cursor.drain_rows())
                    self.description = description
                    self._rows = drained
                    # Mirror execute()'s RETURNING path: rowcount /
                    # lastrowid are accumulated by the underlying cursor
                    # across parameter sets and must flow through the
                    # adapter so SQLAlchemy's Result layer sees them.
                    self.rowcount = rowcount
                    if lastrowid is not None:
                        self.lastrowid = lastrowid
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
                    # ``AsyncCursor.close`` is sync; no ``await_only``
                    # bridge needed.
                    cursor.close()
                except (Exception, asyncio.CancelledError) as exc:
                    peer = _log_safe_peer(self._adapt_connection._connection)
                    logger.debug(
                        "AsyncAdaptedCursor.executemany (id=%s, peer=%s): "
                        "underlying cursor close raised %s; suppressed",
                        id(self),
                        peer,
                        type(exc).__name__,
                        exc_info=True,
                    )

    def fetchone(self) -> tuple[Any, ...] | None:
        # Narrow to the actual row shape. ``Any | None`` collapses to
        # ``Any`` under mypy's gradual typing, defeating the previous
        # narrowing attempt; ``tuple[Any, ...] | None`` matches what
        # the underlying dqlitedbapi sync / async cursors return and
        # makes the None-on-exhaustion PEP 249 contract type-checkable.
        # Runtime behaviour unchanged.
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size: int | None = None) -> Sequence[tuple[Any, ...]]:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        if size is None:
            size = self.arraysize
        if size < 0:
            # Mirror stdlib ``sqlite3.Cursor.fetchmany(-1)`` and the
            # underlying ``dqlitedbapi`` cursor's documented contract:
            # negative size means "fetch all remaining rows". Adapter
            # rows are buffered in-memory, so this is a fast deque
            # drain. Without this parity, cross-driver code using
            # ``fetchmany(-1)`` as "drain all" breaks at the SA layer
            # despite working through the dbapi layer directly.
            return self.fetchall()
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> Sequence[tuple[Any, ...]]:
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

        On a closed cursor raise ``InterfaceError`` rather than
        returning the post-close ``weakref.proxy(...)``: a stale
        consumer reading ``cur.connection`` after close on an
        already-GC'd parent would otherwise see a bare
        ``ReferenceError`` (proxied target collected) which escapes
        the ``dbapi.Error`` hierarchy and SA's
        ``_handle_dbapi_exception`` classifier. The ``_closed`` flag
        is the truth here — NOT
        ``isinstance(self._adapt_connection, weakref.ProxyTypes)``
        which can be True on a still-alive proxy.
        """
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        return self._adapt_connection

    # PEP 249 optional extensions. The sibling ``callproc`` /
    # ``nextset`` / ``scroll`` properties below raise
    # ``NotSupportedError`` because dqlite genuinely has no
    # server-side feature for them; this ``rownumber`` stub raises
    # for a different reason — it is a curated adapter choice, not
    # a feature gap.
    #
    # NOTE: the underlying ``dqlitedbapi.aio.AsyncCursor.rownumber``
    # DOES implement this as a real counter (description-gated
    # 0-based index, returns ``int | None``); it does NOT raise.
    # The adapter does not mirror that because tracking a parallel
    # counter through the deque-pop ownership model would add
    # increment sites in fetchone / fetchmany / fetchall /
    # __next__. Consumers who need rownumber should reach the
    # underlying ``AsyncCursor`` directly (e.g. via the dbapi
    # connection's ``cursor()``).
    #
    # We expose a ``NotSupportedError`` stub property here (rather
    # than leaving the attr absent) so a consumer hard-``getattr``-
    # ing ``cursor.rownumber`` gets a dbapi.Error rather than the
    # bare ``AttributeError`` that would otherwise escape
    # ``except dbapi.Error:``. Mirrors the sibling raise discipline.
    @property
    def rownumber(self) -> int:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        raise NotSupportedError(
            "rownumber is not supported on the SA-adapted async cursor; "
            "use dqlitedbapi.aio.AsyncCursor directly if you need it."
        )

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
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
        raise NotSupportedError("dqlite cursors are not scrollable")

    def __iter__(self) -> Self:
        # Return self so ``iter(cursor) is cursor`` — PEP 234 iterator
        # protocol. The previous generator body (``while self._rows:
        # yield self._rows.popleft()``) produced a fresh generator each
        # time and split iteration into two incompatible paths: the
        # generator popped rows directly while ``__next__`` routed
        # through ``fetchone``. ``__next__`` now drives iteration for
        # both ``for row in cursor`` and ``next(cursor)``; the sibling
        # cursors ``dqlitedbapi.Cursor`` and ``AsyncCursor`` already
        # follow this pattern. Returning ``Self`` (PEP 673) preserves
        # subclass typing through ``iter(cursor)``.
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Self:
        # SA's reference connector cursor and aiosqlite cursor both
        # support the context-manager protocol so callers can
        # ``with conn.execute(...) as cur:``. The body simply yields
        # ``self``; ``__exit__`` closes the cursor. Returns ``Self``
        # (PEP 673) so subclass typing is preserved.
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
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

    No ``_execute_mutex``: SA's reference
    ``AsyncAdapt_dbapi_connection`` (sqlalchemy/connectors/asyncio.py)
    declares ``__slots__ = ("dbapi", "_execute_mutex")`` and wraps
    every per-cursor execute in ``async with
    self._adapt_connection._execute_mutex:``. That mutex exists to
    protect a *long-lived adapter cursor*: the reference connector
    keeps a single dbapi cursor open across calls and the mutex
    serialises greenlets racing on its mutable state.

    This adapter doesn't keep a long-lived cursor — every adapter
    execute opens and closes a fresh dbapi cursor inside a finally —
    AND the underlying ``dqlitedbapi.aio.AsyncConnection.op_lock``
    already serialises commit / execute / rollback at the connection
    layer (see ``dqlitedbapi/aio/connection.py`` ``_op_lock``). The
    mutex would be redundant. If a future change introduces server-
    side cursors / long-lived adapter cursor state, re-introduce
    ``_execute_mutex`` at that point.
    """

    # Parent ``sqlalchemy.engine.interfaces.AdaptedConnection`` declares
    # ``__slots__ = ("_connection",)``; without our own slots declaration
    # each instance gets a ``__dict__`` and defeats the parent's memory
    # optimization (SA's own ``AsyncAdapt_aiosqlite_connection`` follows
    # the same pattern). Add ``dbapi`` to ``__slots__`` to mirror SA's
    # reference ``AsyncAdapt_dbapi_connection``
    # (``sqlalchemy/connectors/asyncio.py:340-347``) which declares
    # ``__slots__ = ("dbapi", "_execute_mutex")`` and stores the dbapi
    # module reference there. Third-party SA-async instrumentation
    # (Sentry / Datadog wrappers, SQLModel, sqlalchemy-utils)
    # introspects ``dbapi_connection.dbapi`` to reach exception classes
    # for type-tagged remap; without the attribute, that introspection
    # falls back to ``AttributeError`` paths.
    __slots__ = ("dbapi",)

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

    @staticmethod
    def _terminate_handled_exceptions() -> tuple[type[BaseException], ...]:
        """Introspection parity with SA's reference at
        ``sqlalchemy/connectors/asyncio.py:417-421``. Third-party SA
        async tooling (Sentry async-pool wrapper, sqlalchemy-utils
        diagnostics) introspects this hook on the connection adapter;
        every other async SA dialect (aiosqlite / asyncpg / aiomysql)
        exposes it, and ``AttributeError`` on dqlite would force those
        tools onto a less-informative fallback path.

        Returns the union of the project-wide transport-class catch
        tuple (``OperationalError`` / ``InterfaceError`` /
        ``DqliteConnectionError`` / ``OSError``) plus
        ``asyncio.CancelledError`` — i.e., the same exceptions that the
        hand-rolled :meth:`terminate` body explicitly suppresses.

        ``terminate()`` itself stays hand-rolled rather than reusing
        SA's ``AsyncAdapt_terminate`` mixin: the dqlite lifecycle
        diverges enough that the mixin would force re-implementing
        both ``_terminate_graceful_close`` and ``_terminate_force_close``
        against an inert template. This method exists purely so
        introspection-only callers see a tuple
        that matches the hand-rolled body's catch arms.
        """
        return _TRANSPORT_CLASS_EXCEPTIONS + (asyncio.CancelledError,)

    def __init__(
        self,
        connection: "AsyncConnection",
        *,
        dbapi: Any = None,
    ) -> None:
        # ``_connection`` is the concrete ``dqlitedbapi.aio.AsyncConnection``
        # this adapter wraps; SQLAlchemy's parent ``AdaptedConnection``
        # declares the attribute with a wider Protocol type, so we keep
        # the store on ``Any`` and rely on the annotation here to document
        # the intended input shape.
        self._connection: Any = connection
        # ``dbapi`` mirrors SA's reference connector — third-party
        # instrumentation hard-``getattr``-s ``dbapi_connection.dbapi``
        # to reach the dbapi module's exception classes. Keyword-only
        # so the existing single-positional construction
        # (``AsyncAdaptedConnection(raw_conn)``) keeps working without
        # touching every call site; default ``None`` is acceptable
        # because dqlite's own code does not read this attribute.
        self.dbapi = dbapi

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

    @property
    def driver_connection(self) -> Any:
        """SA's standard hook for ``event.listens_for(engine.sync_engine,
        "connect")`` callbacks. Inherited from ``AdaptedConnection``;
        the parent returns ``self._connection`` directly. After
        ``close()`` swaps ``self._connection`` for a ``weakref.proxy``,
        a callback that touches the proxy after the inner has been GC'd
        gets ``ReferenceError`` — outside the ``dbapi.Error`` umbrella.
        Mirror the closed-state guard added to ``cursor()`` so the
        post-close path raises ``InterfaceError`` cleanly."""
        if isinstance(self._connection, weakref.ProxyTypes):
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        return self._connection

    def run_async(self, fn: Any) -> Any:
        """SA's ``AdaptedConnection.run_async(fn)`` calls
        ``await_only(fn(self._connection))`` directly. After close,
        ``self._connection`` is a ``weakref.proxy`` whose attribute
        access raises ``ReferenceError`` if the inner has been GC'd —
        not a ``dbapi.Error`` subclass and bypasses SA's exception
        classifier. Surface ``InterfaceError`` up front so cross-driver
        retry middleware sees a clean ``dbapi.Error``."""
        if isinstance(self._connection, weakref.ProxyTypes):
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        return super().run_async(fn)

    def cursor(self, server_side: bool = False) -> AsyncAdaptedCursor:
        # Match the SA connector reference signature
        # (``sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection.cursor``)
        # which takes ``server_side: bool = False``. The dialect pins
        # ``supports_server_side_cursors=False`` so SA itself never
        # passes ``server_side=True`` here, but third-party callers
        # and future SA paths may; raise ``NotSupportedError`` (a
        # PEP 249 ``dbapi.Error`` subclass) so the rejection routes
        # through SA's ``_handle_dbapi_exception`` classifier and
        # cross-driver ``except dbapi.Error:`` clauses catch it.
        # Sibling cursor surface (``callproc`` / ``nextset`` /
        # ``scroll``) follows the same discipline.
        if server_side:
            raise NotSupportedError(
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
        parameters: Sequence[Any] | None = None,
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
        # Single-discipline error routing: open the cursor AND run
        # the inner execute inside one try-frame so any fault —
        # whether from ``self.cursor()`` (proxy guard, loop-bound
        # delegate failures) or ``cur.execute()`` itself — goes
        # through ``_handle_exception``. The cursor-level execute
        # (``AsyncAdaptedCursor.execute``) wraps its frame the same
        # way; without symmetric routing here, a cross-loop
        # ``RuntimeError`` from ``cursor()`` would leak raw past
        # SA's ``is_disconnect`` classifier when invoked via the
        # connection-level convenience path (provision /
        # do_terminate) while the engine-level path (Result layer)
        # remaps it. The outer try-frame closes any cursor we
        # opened before propagating the remapped or unmapped
        # exception.
        cur: AsyncAdaptedCursor | None = None
        try:
            try:
                cur = self.cursor()
                if parameters is None:
                    cur.execute(operation)
                else:
                    cur.execute(operation, parameters)
            except BaseException as error:
                self._handle_exception(error)
        except BaseException:
            if cur is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    cur.close()
            raise
        assert cur is not None  # _handle_exception either raises or never returns
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

        **Type signature divergence from SA reference**: this hook
        accepts ``BaseException``, while SA's reference connector
        (``connectors/asyncio.py:365``) and aiosqlite's adapter
        (``dialects/sqlite/aiosqlite.py:333``) type the parameter as
        ``Exception``. The wider ``BaseException`` type is deliberate:
        the cursor-level catch sites in this adapter
        (``execute`` / ``executemany`` at ``except BaseException as
        error``) route every cursor-level error through this hook so
        the surrounding ``finally`` always runs (closing the freshly-
        opened underlying cursor). The ``isinstance(error,
        (RuntimeError, ProgrammingError))`` short-circuit at the top
        of this body skips KeyboardInterrupt / SystemExit /
        CancelledError, falling through to ``raise error`` —
        preserving propagation for callers. Narrowing to
        ``Exception`` would require reshaping the cursor-level
        catches AND prove load-bearing-equivalence for cancel-during-
        execute paths; the wider type holds the diagnostic-leak
        prevention contract.

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
        # Walk the ``__cause__`` / ``__context__`` chain (plus PEP 654
        # group children) up to the shared depth budget so a wrapping
        # layer that re-raised the cross-loop / loop-closed /
        # nested-loop fault as a different type does not silently
        # bypass the remap. Mirrors the ``is_disconnect`` discipline
        # in base.py (which uses the same ``_walk_cause_chain``
        # helper); without this walk, an explicit
        # ``raise FooError(...) from RuntimeError("Event loop is
        # closed")`` would leak the original RuntimeError past SA's
        # classifier (gated on ``DatabaseError``) and the pool would
        # retain the broken slot.
        #
        # Lower-case once per hop so substring scans below are
        # case-insensitive, mirroring base.py's ``is_disconnect``
        # discipline (the ``_dqlite_disconnect_messages`` tuple is
        # stored lower-cased and the call site lower-cases the
        # candidate text once). Without this, a Python minor that
        # flipped the canonical wording's casing (e.g.
        # ``Event Loop Is Closed``) would silently bypass the
        # remap. The original message is preserved verbatim in the
        # remapped wording's tail.
        for hop in _walk_cause_chain(error):
            if not isinstance(hop, (RuntimeError, ProgrammingError)):
                continue
            msg = str(hop)
            msg_lower = msg.lower()
            # Both substrings are needed: ``"different loop"`` matches
            # Python's ``"attached to a different loop"`` and any
            # variant that uses the bare phrase, while
            # ``"different event loop"`` matches dqlitedbapi's distinct
            # wording. The redundant ``or "attached to a different
            # loop"`` clause from the older arm was dropped — that
            # phrase strictly contains ``"different loop"`` so the
            # first check already matches it.
            if "different loop" in msg_lower or "different event loop" in msg_lower:
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
            # fresh connection. The remapped wording ``"event loop
            # closed"`` is the substring matched by
            # ``_dqlite_disconnect_messages`` in base.py; any change
            # to that wording must keep the substring in sync.
            if "event loop is closed" in msg_lower:
                raise OperationalError(f"event loop closed: {msg}", code=None) from error
            # ``RuntimeError("This event loop is already running")``
            # surfaces when third-party glue calls ``await_only`` from
            # a context that already has a running loop on the same
            # thread (asyncio rejects nested loop entry). Without
            # remap, the bare RuntimeError leaks past SA's
            # ``is_disconnect`` classifier (gated on DatabaseError)
            # and the pool retains the broken slot. Treat as a
            # transport-class disconnect so the slot invalidates.
            # Substring is added to ``_dqlite_disconnect_messages`` in
            # base.py for symmetric classification.
            if "loop is already running" in msg_lower:
                raise OperationalError(f"event loop already running: {msg}", code=None) from error
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
            try:
                self._force_close_transport()
            finally:
                self._release_inner_strong_ref()
            return

        # Outer try/finally guarantees the post-close ``weakref.proxy``
        # swap runs on EVERY exit arm — normal return, early return
        # from the rollback-loop-closed handler, raises from
        # ``_handle_exception`` remap, transport-class fallthrough, and
        # cancel re-raise. Without it, the rollback-arm
        # ``RuntimeError("Event loop is closed")`` ``return`` (and the
        # ``_handle_exception``-raise / cancel-raise arms) bypass the
        # swap, leaving SA's pool diagnostic ring + pytest session-
        # fixture cache pinning the inner ``AsyncConnection`` (and
        # through it the client-layer state, registered
        # ``weakref.finalize``, and frame-pinning
        # ``_invalidation_cause``). The release discipline applies on
        # every exit arm, success or failure — the adapter is dead
        # post-close regardless of how it got there.
        try:
            # Attempt rollback before close so a caller that exits
            # without committing does not leave a dangling server-
            # side transaction. The underlying async connection's
            # rollback is a silent no-op when no transaction is
            # active and when the connection has never been used, so
            # the double-call is safe.
            #
            # Narrow the suppression to the categories a best-effort
            # rollback can legitimately raise — connection-level /
            # transport errors — so programming bugs (AttributeError,
            # TypeError, bare RuntimeError, etc.) still propagate.
            # ``ConnectionError``, ``BrokenPipeError``, and
            # ``TimeoutError`` are all ``OSError`` subclasses (since
            # Python 3.3+/3.10+ respectively), so a single ``OSError``
            # check covers every stdlib transport-error shape —
            # matching the source-of-truth classification in
            # ``base.py``'s ``is_disconnect``.
            #
            # Inner try/finally so close() runs regardless of how
            # rollback() exits — narrow-caught, programming bug, or
            # ``BaseException`` like ``CancelledError`` during pool
            # dispose. SA's pool does not re-call close() on failure,
            # so skipping close would leak the underlying
            # AsyncConnection. Mirror of the inverse leak fixed in
            # DqliteDialect_aio.connect().
            try:
                try:
                    await_only(self._connection.rollback())
                except _TRANSPORT_CLASS_EXCEPTIONS as exc:
                    # Silent suppression used to hide e.g. "leader flip
                    # mid-rollback" from operators — a DEBUG line
                    # preserves the diagnostic without masking or
                    # propagating. Include both id(self) and the peer
                    # address so a noisy pool can be correlated to
                    # specific adapter instances and nodes.
                    peer = _log_safe_peer(self._connection)
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
                    # remap as commit/rollback/execute/executemany so
                    # SA's is_disconnect classifier (which is gated on
                    # DatabaseError) sees an OperationalError instead
                    # of a bare RuntimeError. Without this, cross-loop
                    # close() would propagate an un-classified
                    # RuntimeError past engine.dispose(). The outer
                    # try/finally still runs the proxy swap on the
                    # raise path.
                    # Lowercase once at the top of the arm so the
                    # substring scans below mirror the
                    # ``_handle_exception`` / ``is_disconnect``
                    # ``.lower()`` discipline (d8ecb49). The CPython
                    # source-of-truth for both phrases is at the
                    # asyncio-internals level, not a stable documented
                    # API; a future point-release capitalisation tweak
                    # would otherwise silently bypass the remap and
                    # leak bare ``RuntimeError`` past
                    # ``engine.dispose()``.
                    msg_lower = str(exc).lower()
                    if "different loop" in msg_lower or "different event loop" in msg_lower:
                        self._handle_exception(exc)
                    # ``RuntimeError("Event loop is closed")`` lands
                    # here during ``engine.dispose()`` after a per-call
                    # ``asyncio.run()`` finished and tore the loop down
                    # — symmetric with the close arm below. The
                    # ``has_terminate=True`` dialect promise says
                    # close()/dispose must not propagate failures from
                    # this path; debug-log and return. The outer
                    # try/finally still runs the proxy swap on this
                    # return path. The debug log preserves the
                    # traceback for triage.
                    if "event loop is closed" in msg_lower:
                        peer = _log_safe_peer(self._connection)
                        logger.debug(
                            "AsyncAdaptedConnection.close (id=%s, peer=%s): "
                            "rollback raised RuntimeError (%s); skipping close",
                            id(self),
                            peer,
                            type(exc).__name__,
                            exc_info=True,
                        )
                        return
                    # ``RuntimeError("This event loop is already running")``
                    # surfaces when third-party glue calls ``await_only``
                    # from a context that already has a running loop on
                    # the same thread (asyncio rejects nested loop
                    # entry). The ``"loop is already running"`` substring
                    # arm of ``_handle_exception`` and
                    # ``_dqlite_disconnect_messages`` in base.py both
                    # cover this phrase; route through the same remap so
                    # the close-arm matches that discipline rather than
                    # leaking a bare ``RuntimeError`` past
                    # ``engine.dispose()``.
                    if "loop is already running" in msg_lower:
                        self._handle_exception(exc)
                    # Other RuntimeErrors (programmer bugs) propagate.
                    raise
                # The non-greenlet path is handled by the
                # ``in_greenlet()`` preflight at the top of ``close()``;
                # ``MissingGreenlet`` cannot land here.
                # ``CancelledError`` from the rollback await is allowed
                # to propagate so the cancellation signal is preserved
                # — the finally below still runs close(), and the
                # close arm's CancelledError catch routes through the
                # sync force-close fallback before re-raising.
                # Suppressing here would convert a still-active cancel
                # into a clean return, contradicting asyncio's
                # "cancellation propagates" contract; the prior test
                # ``test_close_runs_close_after_rollback_raise.py``
                # pins that contract.
            finally:
                # Narrow the close-time exception set to transport-
                # class failures. A transient OSError /
                # DqliteConnectionError mid-close must not escape
                # do_close and abort engine.dispose(). Matches the
                # rollback branch's classification. Programmer bugs
                # (AttributeError / TypeError) still propagate.
                try:
                    await_only(self._connection.close())
                except _TRANSPORT_CLASS_EXCEPTIONS as exc:
                    peer = _log_safe_peer(self._connection)
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
                    # land here during ``engine.dispose()`` after a
                    # per-call ``asyncio.run()`` finished and tore the
                    # loop down. The async machinery cannot run; reap
                    # the writer synchronously so the transport
                    # doesn't leak. ``has_terminate=True`` (the
                    # dialect-level promise) means close()/dispose
                    # must not propagate failures from this path; the
                    # debug log preserves the traceback for triage.
                    peer = _log_safe_peer(self._connection)
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
                    # Cancel landing on the close await (canonical
                    # trigger: an outer ``asyncio.timeout`` mid-
                    # ``engine.dispose()`` under SIGTERM-with-budget).
                    # Run the sync transport fallback so the writer
                    # is closed even though the async machinery was
                    # interrupted, then re-raise so the cancel still
                    # propagates to the caller. The outer try/finally
                    # still runs the proxy swap on the raise path.
                    self._force_close_transport()
                    raise
        finally:
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
            self._release_inner_strong_ref()

    def _release_inner_strong_ref(self) -> None:
        """Swap ``self._connection`` for a ``weakref.proxy`` of itself.

        Centralised so both ``close()`` and ``terminate()`` share the
        same release discipline; symmetric with how the dbapi layer's
        ``AsyncConnection.close`` swaps its own loop-bound state.

        Suppression covers two corner cases: ``TypeError`` for inner
        types that don't support weakref (always supported for
        ``AsyncConnection``; defensive for hand-rolled test doubles)
        and ``ReferenceError`` for the rare case where this method is
        called twice on the same adapter and the inner has already
        been GC'd between calls (``weakref.proxy(dead_proxy)`` raises
        ``ReferenceError``, not ``TypeError``).
        """
        with contextlib.suppress(TypeError, ReferenceError):
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
        The two ``getattr`` reads on ``self._connection`` sit INSIDE
        the try frame so a ``ReferenceError`` from a dead
        ``weakref.proxy`` (the post-``_release_inner_strong_ref``
        state) is absorbed with the same swallow-
        and-log discipline as a hook-side failure — the docstring
        contract "Never raises" holds at the boundary the inherited
        ``DqliteDialect.do_close`` fallback's
        ``contextlib.suppress(*_TRANSPORT_CLASS_EXCEPTIONS)`` relies
        on (``ReferenceError`` is intentionally NOT in that tuple
        because it is a GC-lifecycle hazard, not a transport class).
        """
        peer: object | None = None
        try:
            peer = _log_safe_peer(self._connection)
            hook = getattr(self._connection, "force_close_transport", None)
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
        except (Exception, asyncio.CancelledError) as exc:  # pragma: no cover - defensive
            # Narrow to ``(Exception, asyncio.CancelledError)`` so a
            # greenlet-level cancel from SA's pool-dispose path is
            # still absorbed (``CancelledError`` is a ``BaseException``
            # subclass since 3.8) but ``KeyboardInterrupt`` /
            # ``SystemExit`` propagate. Mirrors the sibling cursor-
            # close discipline upstream in this module.
            logger.debug(
                "AsyncAdaptedConnection._force_close_transport (id=%s, peer=%s): "
                "best-effort sync close raised (%s); ignoring",
                id(self),
                peer,
                type(exc).__name__,
                exc_info=True,
            )

    def force_close_transport(self) -> None:
        """Public alias of :meth:`_force_close_transport`.

        The inherited :meth:`DqliteDialect.do_close` fallback (see
        ``base.py``) reaches for ``dbapi_connection.force_close_transport()``
        — the public name, matching the dbapi ``Connection.force_close_transport``
        on the sync side. ``DqliteDialect_aio`` inherits ``do_close``
        unmodified, so without this public surface a sync-pool teardown
        path that reached the transport-class fallback on an
        ``AsyncAdaptedConnection`` (e.g. cross-loop dispose via
        ``engine.dispose()``) would raise ``AttributeError`` —
        ``AttributeError`` is NOT in ``_TRANSPORT_CLASS_EXCEPTIONS`` so
        it would escape the ``contextlib.suppress`` and the transport
        would leak.

        The method delegates straight to :meth:`_force_close_transport`
        which already encapsulates the sync teardown semantics (the
        underscore variant remains the in-module call shape because
        every internal call site and existing test pin references it
        by that name).
        """
        self._force_close_transport()

    def terminate(self) -> None:
        """Force-close the underlying connection without rollback.

        SQLAlchemy's async pool calls ``dialect.do_terminate(dbapi_conn)``
        (which defers to this method) when ``has_terminate = True`` and
        a connection must be forcibly reclaimed — typically during
        ``engine.dispose()`` under failure, or when a stuck rollback
        would otherwise block shutdown. Unlike ``close()`` we do NOT
        attempt rollback first: that's the whole point of terminate.

        Mirrors ``close()``'s post-close ``weakref.proxy`` swap on
        every exit arm — SA's pool invalidate path uses terminate, and
        the same diagnostic-ring / fixture-pinning concern that
        motivates close()'s swap applies symmetrically here.
        """
        # Preflight on ``in_greenlet()`` — see ``close()`` for
        # rationale. Non-greenlet finalize paths reap the writer
        # synchronously without paying the ``MissingGreenlet``
        # exception-allocation cost.
        if not in_greenlet():
            try:
                self._force_close_transport()
            finally:
                self._release_inner_strong_ref()
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
        #
        # Outer try/finally guarantees the post-close ``weakref.proxy``
        # swap runs on every exit arm — symmetric with ``close()``.
        try:
            try:
                await_only(self._connection.close())
            except _TRANSPORT_CLASS_EXCEPTIONS as exc:
                peer = _log_safe_peer(self._connection)
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
                # ``asyncio.run()`` per-call pattern tears the loop
                # down, then SA's pool finalizer calls ``terminate()``
                # and the async machinery raises
                # ``RuntimeError("Event loop is closed")``.
                # ``has_terminate=True`` promises SA that dispose never
                # propagates failures from this path; reap the writer
                # synchronously and stay quiet (DEBUG only).
                peer = _log_safe_peer(self._connection)
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
        finally:
            self._release_inner_strong_ref()


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

        ``has_terminate=True`` promises SA a non-raising path; suppress
        any tail ``Exception`` so SA's pool finalize cannot crash on a
        partial-state connection (mirrors the sync sibling's
        suppression discipline at ``base.py``). ``asyncio.CancelledError``
        is deliberately NOT caught — asyncio's structured-concurrency
        contract says cancels must propagate, and an outer cancel
        signalling "abort dispose now" must not be silently swallowed.
        """
        peer = _log_safe_peer(dbapi_connection)
        try:
            dbapi_connection.terminate()
        except Exception:  # terminate must not raise
            logger.debug(
                "do_terminate: terminate raised on dispose for peer=%s id=%s; "
                "proceeding (has_terminate=True non-raising contract)",
                peer,
                id(dbapi_connection),
                exc_info=True,
            )

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Async-side bespoke ping.

        Inheriting the sync ``DqliteDialect.do_ping`` from base.py
        routes through ``AsyncAdaptedCursor`` and pays three
        ``await_only`` hops per checkout — ``cursor.execute("SELECT 1")``
        plus ``cursor.fetchall()`` (the description-truthy path
        materialises ``SELECT 1``'s row) plus ``cursor.close()``. Worse,
        loop-state ``RuntimeError`` from a closed loop reaches the
        sync caller without going through the adapter's
        ``_handle_exception``, so SA's ``is_disconnect`` classifier
        (gated on ``DatabaseError``) misses it and the broken slot
        survives.

        Run ``SELECT 1`` directly through the dbapi async cursor
        instead — one execute + one fetch + one close, all under a
        single ``await_only`` hop, with explicit RuntimeError routing
        through ``_handle_exception`` so loop-state shapes classify as
        ``OperationalError`` and SA evicts the slot. Mirrors asyncpg's
        ``_async_ping`` shape (asyncpg.py:814).

        The exception arms preserve the sync ``do_ping``'s
        ``_BARE_DBE_DISCONNECT_CODES`` arm so codes 11/24/26
        (CORRUPT/FORMAT/NOTADB) still classify as ping-fail.
        """
        from dqliteclient import DqliteConnectionError
        from dqlitedbapi import DatabaseError
        from sqlalchemydqlite.base import _BARE_DBE_DISCONNECT_CODES

        try:
            await_only(self._async_ping(dbapi_connection))
        except (
            OperationalError,
            ProgrammingError,
            InterfaceError,
            DqliteConnectionError,
            OSError,
        ):
            return False
        except DatabaseError as exc:
            if getattr(exc, "code", None) in _BARE_DBE_DISCONNECT_CODES:
                return False
            raise
        except RuntimeError:
            # ``_handle_exception`` remaps three known loop-state
            # RuntimeError phrasings into ``OperationalError``; any
            # other RuntimeError (a future Python wording change, an
            # ``asyncio.get_running_loop`` failure, a ``Task got bad
            # yield`` shape, ``await_only`` pre-coroutine surfaces)
            # would otherwise escape ``do_ping`` entirely. SA's
            # ``_do_ping_w_event`` catches only ``loaded_dbapi.Error``
            # and would not invalidate the slot. Treat any
            # RuntimeError on the ping path as slot-fatal — same
            # posture as the ``OSError`` catch above (transport-class
            # faults retire the slot).
            return False
        return True

    async def _async_ping(self, dbapi_connection: Any) -> None:
        """Async leg of ``do_ping``: open a cursor, run ``SELECT 1``,
        fetch one row, close. ``cursor()`` on the dbapi
        ``AsyncConnection`` is synchronous (returns an ``AsyncCursor``);
        ``execute`` / ``fetchone`` on the cursor are coroutines;
        ``close`` is synchronous by design (see ``AsyncCursor.close``
        docstring — sync to surface forgot-await as a sharp error
        rather than a silent no-op).

        Route any ``RuntimeError`` through the adapter's
        ``_handle_exception`` so loop-state shapes (different-loop,
        loop-closed) re-raise as ``OperationalError`` — the outer
        ``do_ping`` then catches that as ping-fail.

        Routing scope: only ``RuntimeError`` is remapped here.
        Any ``dbapi.Error`` subclass that escapes the inner block
        (notably a cross-loop ``ProgrammingError`` from
        ``AsyncConnection._ensure_locks``, or a ``ProgrammingError``
        from ``cursor()`` itself) is allowed to propagate to the
        outer ``do_ping``, which catches
        ``(OperationalError, ProgrammingError, InterfaceError,
        DqliteConnectionError, OSError)`` and returns ``False`` so
        the pool retires the slot. The narrow routing here is
        deliberate — DO NOT broaden it without also reviewing
        ``do_ping``'s outer catch list. If that list ever drops
        ``ProgrammingError``, this ``except`` must be widened to
        ``(RuntimeError, ProgrammingError)`` to preserve the
        ping-failure / slot-invalidation chain.
        """
        # Closed-state guard mirroring ``AsyncAdaptedConnection.cursor``'s
        # closed-state guard: ``close()`` replaces ``self._connection``
        # with ``weakref.proxy(...)``. Reaching into ``_connection.cursor()``
        # directly would surface ``ReferenceError`` if the proxied
        # inner has been GC'd — not a ``dbapi.Error`` subclass and
        # would escape the outer ``do_ping`` classifier
        # (``OperationalError, ProgrammingError, InterfaceError,
        # DqliteConnectionError, OSError``). Translate to
        # ``InterfaceError`` up front so cross-driver retry middleware
        # and SA's ``_handle_dbapi_exception`` see a clean
        # ``dbapi.Error``.
        if isinstance(dbapi_connection._connection, weakref.ProxyTypes):
            raise InterfaceError(f"Connection is closed (id={id(dbapi_connection)})")
        try:
            cur = dbapi_connection._connection.cursor()
            try:
                await cur.execute(self._dialect_specific_select_one)
                await cur.fetchone()
            finally:
                # Mirror the sibling cursor-close discipline in
                # ``AsyncAdaptedCursor.execute`` (this module, search
                # for ``except (Exception, asyncio.CancelledError)``):
                # narrow to that tuple so a greenlet-level cancel is
                # still absorbed (``CancelledError`` is a
                # ``BaseException`` subclass since 3.8) but
                # ``KeyboardInterrupt`` / ``SystemExit`` propagate.
                # The previous ``contextlib.suppress(Exception)``
                # silently absorbed dbapi disconnect-class errors
                # raised from the close round-trip (CORRUPT / FORMAT
                # / NOTADB); the DEBUG log here makes those failures
                # observable so a flapping leader is visible in
                # operator logs even when the ping itself appears
                # successful. Suppression scope still includes those
                # errors (the ping already ran successfully, so
                # retiring the slot now would defeat the whole point
                # of pre-ping) but they are no longer silent.
                peer = _log_safe_peer(dbapi_connection._connection)
                try:
                    # ``AsyncCursor.close`` is sync — see its
                    # docstring. No await needed.
                    cur.close()
                except (Exception, asyncio.CancelledError) as exc:
                    logger.debug(
                        "_async_ping cursor close (id=%s, peer=%s): %s; suppressed",
                        id(dbapi_connection),
                        peer,
                        type(exc).__name__,
                        exc_info=True,
                    )
        except RuntimeError as error:
            dbapi_connection._handle_exception(error)

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """Async-side fast-path on already-closed adapter connections.

        ``AsyncAdaptedConnection.close`` replaces ``self._connection``
        with ``weakref.proxy(...)`` (documented rationale: SA's pool
        diagnostic ring otherwise pins frame state and prevents GC).
        When SA calls ``is_disconnect`` after a failure on such a
        connection, the inner connection is already torn down — the
        truthful answer is ``True`` regardless of what the cause chain
        says.

        Mirrors asyncpg's ``connection._connection.is_closed()``
        short-circuit (``sqlalchemy/dialects/postgresql/asyncpg.py:1172``).
        We already use the proxy-type check in
        :meth:`AsyncAdaptedConnection.cursor` for the same "is the
        inner connection torn down" question, so reusing it here is a
        single-source-of-truth choice.

        The non-fast-path (``connection`` is None, not an adapter, or
        holds a live inner connection) falls through to
        ``super().is_disconnect`` so the rich type/code/substring
        classifier in ``DqliteDialect.is_disconnect`` runs unchanged.
        """
        if (
            connection is not None
            and isinstance(connection, AsyncAdaptedConnection)
            and isinstance(connection._connection, weakref.ProxyTypes)
        ):
            return True
        return super().is_disconnect(e, connection, cursor)

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

        SA convention (asyncpg.py:937, aiosqlite.py:399, aiomysql):
        callers can inject a custom async-connection factory via
        ``connect_args={"async_creator_fn": my_factory}``. When
        present, ``my_factory(*args, **kwargs)`` is invoked instead of
        ``loaded_dbapi.connect`` and is expected to return an object
        exposing the ``AsyncConnection`` shape (``connect``, ``cursor``,
        ``commit``, ``rollback``, ``close`` — the surface
        ``AsyncAdaptedConnection`` calls into). The pop must precede
        ``_validate_connect_kwargs`` because the strict allowlist
        would otherwise reject the hook key with ``ArgumentError``.

        Note our two-step shape is structurally different from
        asyncpg/aiosqlite: ``loaded_dbapi.connect`` is a SYNC factory
        returning a not-yet-connected ``AsyncConnection``; the actual
        transport open is the ``await_only(raw_conn.connect())``
        below. A ``creator_fn`` whose return value already has an
        open transport should expose ``connect()`` as an idempotent
        no-op coroutine — the dbapi's own ``AsyncConnection.connect``
        already has that property when called twice, so a creator
        that wraps a pre-built dbapi connection works without
        modification.

        **Contract for third-party ``async_creator_fn``** (BREAKAGE
        WARNING): the returned object's ``connect()`` is invoked
        unconditionally below — once by us, possibly already by the
        creator. Any of these shapes are safe:

        - The creator returns a NOT-YET-CONNECTED dbapi
          ``AsyncConnection``. We open the transport for it. ✓
        - The creator returns an ALREADY-CONNECTED dbapi
          ``AsyncConnection``. ``AsyncConnection.connect`` checks
          ``self._async_conn is not None`` and returns the existing
          inner conn — idempotent no-op. ✓
        - The creator returns a CUSTOM async-connection-shaped
          object whose ``connect()`` is NOT idempotent: this WILL
          double-connect (open twice / fail). The creator-provided
          ``connect()`` must be coroutine-shaped AND idempotent —
          either by short-circuiting on a "already connected"
          flag or by being a no-op coroutine when called against
          a live transport.

        We do NOT skip the ``raw_conn.connect()`` when the creator
        is provided (matching SA's ``aiosqlite.py:399`` shape would
        risk leaving a creator-returned-unopened conn without a
        transport). Custom creators must satisfy the idempotency
        contract above.
        """
        creator_fn = cparams.pop("async_creator_fn", None)
        self._validate_connect_kwargs(cparams)
        # Cover the construction itself with the same try frame so a
        # ``BaseException`` (KeyboardInterrupt / SystemExit) delivered
        # between the assignment to ``raw_conn`` and the ``try:`` cannot
        # leak the freshly-built ``AsyncConnection`` (registered locks,
        # ``weakref.finalize`` ResourceWarning surface) without orderly
        # cleanup. Mirrors the project-wide
        # construct-inside-the-try-frame discipline applied to other
        # eager-allocation paths (cluster / pool comprehensions).
        raw_conn: Any = None
        try:
            if creator_fn is not None:
                raw_conn = creator_fn(*cargs, **cparams)
            else:
                raw_conn = self.loaded_dbapi.connect(*cargs, **cparams)
            await_only(raw_conn.connect())
        except BaseException:
            if raw_conn is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    AsyncAdaptedConnection(raw_conn, dbapi=self.loaded_dbapi).terminate()
            raise
        return AsyncAdaptedConnection(raw_conn, dbapi=self.loaded_dbapi)

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the underlying driver-level connection."""
        return connection._connection
