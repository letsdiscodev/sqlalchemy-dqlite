"""Async dqlite dialect for SQLAlchemy."""

import asyncio
import contextlib
import inspect
import logging
import types
import weakref
from collections import deque
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, Self

from sqlalchemy import pool
from sqlalchemy.engine import URL, AdaptedConnection
from sqlalchemy.exc import ArgumentError
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util import await_only
from sqlalchemy.util.concurrency import in_greenlet

from dqliteclient import DqliteConnectionError
from dqlitedbapi import (
    DatabaseError,
    DescriptionTuple,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemydqlite.base import (
    _AUTOCOMMIT_REJECTION_MSG,
    _BARE_DBE_DISCONNECT_CODES,
    _TRANSPORT_CLASS_EXCEPTIONS,
    DqliteDialect,
    _do_terminate_logging,
    _is_int_not_bool,
    _log_safe_peer,
    _walk_cause_chain,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

__all__ = ["AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"]

# Sentinel discriminating "second positional omitted" from an explicit
# ``connection=None`` in ``AsyncAdaptedConnection.__init__``.
_UNSET: Any = object()

type _Description = Sequence[DescriptionTuple] | None


def _remap_loop_state_runtime_error(error: BaseException) -> None:
    """Re-raise any loop-state ``RuntimeError``/``ProgrammingError`` in the
    cause chain as ``OperationalError`` so ``is_disconnect`` sees it; returns
    normally (no raise) if nothing matches.

    Remapped wording must stay in sync with ``_dqlite_disconnect_messages`` in
    ``base.py``. The close-arm deliberately does NOT route its "event loop is
    closed" case here: the helper always raises, which would break close()'s
    ``has_terminate=True`` must-not-raise contract.
    """
    # ``from hop`` (not ``from error``): bounded-depth chain walkers need the
    # loop-state shape at __cause__ depth 1. Mirrors cursor.py::_call_client.
    for hop in _walk_cause_chain(error):
        if not isinstance(hop, (RuntimeError, ProgrammingError)):
            continue
        msg = str(hop)
        msg_lower = msg.lower()
        if "different loop" in msg_lower or "different event loop" in msg_lower:
            raise OperationalError(f"event-loop mismatch: {msg}", code=None) from hop
        if "event loop is closed" in msg_lower:
            raise OperationalError(f"event loop closed: {msg}", code=None) from hop
        if "loop is already running" in msg_lower:
            raise OperationalError(f"event loop already running: {msg}", code=None) from hop


class AsyncAdaptedCursor:
    """Adapts an AsyncCursor for SQLAlchemy's greenlet-based async engine.

    Eagerly buffers all rows at execute() time, then serves fetch* calls
    synchronously from the deque (matches SA's aiosqlite dialect). ``arraysize``
    governs only the fetchmany chunk size, not memory footprint.

    Stores description/rowcount/lastrowid as plain attributes rather than SA's
    reference ``@property`` delegation because each execute opens and closes a
    fresh underlying cursor — there is no live cursor to delegate to. Exposes
    only the sync context-manager/iterator protocol; for native async-cm /
    async-iteration reach the underlying ``dqlitedbapi.aio.AsyncCursor``.
    """

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
        self._closed: bool = False

    async def _async_soft_close(self) -> None:
        return

    def _suppress_close_on_execute(self, cursor: "AsyncCursor", method_name: str) -> None:
        """Close ``cursor``, suppressing/logging any close failure so it can't
        replace a primary execute exception (covers greenlet cancel; KI/SystemExit
        still propagate)."""
        try:
            cursor.close()
        except (Exception, asyncio.CancelledError) as exc:
            peer = _log_safe_peer(self._adapt_connection._connection)
            logger.debug(
                "AsyncAdaptedCursor.%s (id=%s, peer=%s): "
                "underlying cursor close raised %s; suppressed",
                method_name,
                id(self),
                peer,
                type(exc).__name__,
                exc_info=True,
            )

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        # Rejects bool/non-int (dqlite footgun guard); 0 and negative accepted
        # to match SA's reference adapter and stdlib sqlite3.
        if not _is_int_not_bool(value):
            raise ProgrammingError(f"arraysize must be an int, got {value!r}")
        self._arraysize = value

    def close(self) -> None:
        """Idempotent close; preserves rowcount/lastrowid post-close so SA's
        Result layer can read ``cursor.lastrowid`` after an INSERT."""
        # Short-circuit makes idempotency structural rather than relying on the
        # TypeError suppression below (double-wrapping a proxy raises TypeError).
        if self._closed:
            return
        # Clear result-set surface but PRESERVE rowcount/lastrowid — SA's
        # Result layer reads lastrowid lazily after close.
        self.description = None
        self._rows.clear()
        self._closed = True
        # weakref.proxy the back-references so a retained closed cursor doesn't
        # pin the inner AsyncConnection (and its client-layer state).
        with contextlib.suppress(TypeError):
            self._adapt_connection = weakref.proxy(self._adapt_connection)
        with contextlib.suppress(TypeError):
            self._connection = weakref.proxy(self._connection)

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> None:
        # ``parameters`` is Sequence|None: the dbapi is qmark-only and rejects
        # mappings, and SA's compiler always hands a sequence to qmark dialects.
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        # Clear buffered state FIRST so cancellation mid-execute leaves a
        # "no active result" state. Do NOT clear lastrowid (sticky-INSERT
        # contract: it survives non-INSERT executes and close, matching stdlib).
        self.description = None
        self.rowcount = -1
        self._rows.clear()

        # cursor() inside the try so a synchronous raise (closed conn,
        # cross-loop ProgrammingError) routes through _handle_exception too.
        cursor: AsyncCursor | None = None
        try:
            try:
                cursor = self._connection.cursor()
                # Forward-compat scaffolding for any future wire-prefetch tuning.
                cursor.arraysize = self._arraysize
                if parameters is not None:
                    await_only(cursor.execute(operation, parameters))
                else:
                    await_only(cursor.execute(operation))

                if cursor.description:
                    # Atomic-on-success: capture into locals, run destructive
                    # drain_rows last, then assign together — a mid-drain raise
                    # leaves the no-result baseline, not a half-populated empty
                    # result. drain_rows transfers the buffer without a fetchall
                    # copy (halves peak memory on large INSERT...RETURNING).
                    description = cursor.description
                    rowcount = cursor.rowcount
                    lastrowid = cursor.lastrowid
                    drained = deque(cursor.drain_rows())
                    self.description = description
                    self._rows = drained
                    self.rowcount = rowcount
                    if lastrowid is not None:
                        self.lastrowid = lastrowid
                else:
                    if cursor.lastrowid is not None:
                        self.lastrowid = cursor.lastrowid
                    self.rowcount = cursor.rowcount
            except BaseException as error:
                # Centralised remap of driver-layer quirks (loop-mismatch etc.).
                self._adapt_connection._handle_exception(error)
        finally:
            if cursor is not None:
                self._suppress_close_on_execute(cursor, "execute")

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> None:
        """Materialises ``seq_of_parameters`` to a list so SA's disconnect
        retry path can re-iterate it (a one-shot iterator would re-issue as a
        silent zero-row execute + COMMIT — data loss). lastrowid follows the
        sticky-INSERT contract (see ``execute``), which DIVERGES from SA's
        aiosqlite reference that clears it unconditionally on non-INSERT.
        """
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        # isinstance gate keeps SA's always-a-list call a no-copy fast path.
        if not isinstance(seq_of_parameters, list):
            seq_of_parameters = list(seq_of_parameters)
        self.description = None
        self.rowcount = -1
        self._rows.clear()

        cursor: AsyncCursor | None = None
        try:
            try:
                cursor = self._connection.cursor()
                cursor.arraysize = self._arraysize
                await_only(cursor.executemany(operation, seq_of_parameters))
                # RETURNING: the cursor accumulates rows across param sets;
                # capture them or insertmanyvalues+RETURNING loses every row.
                if cursor.description:
                    description = cursor.description
                    rowcount = cursor.rowcount
                    lastrowid = cursor.lastrowid
                    drained = deque(cursor.drain_rows())
                    self.description = description
                    self._rows = drained
                    self.rowcount = rowcount
                    if lastrowid is not None:
                        self.lastrowid = lastrowid
                else:
                    if cursor.lastrowid is not None:
                        self.lastrowid = cursor.lastrowid
                    self.rowcount = cursor.rowcount
            except BaseException as error:
                self._adapt_connection._handle_exception(error)
        finally:
            if cursor is not None:
                self._suppress_close_on_execute(cursor, "executemany")

    def fetchone(self) -> tuple[Any, ...] | None:
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
            # Negative size = "fetch all", matching stdlib sqlite3 and the dbapi.
            return self.fetchall()
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> Sequence[tuple[Any, ...]]:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        retval = list(self._rows)
        self._rows.clear()
        return retval

    def setinputsizes(self, *args: Any) -> None:
        # No-op (dqlite ignores sizing hints); variadic *args accepts both
        # PEP 249 single-sequence and SA's variadic call shapes.
        del args
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")

    @property
    def connection(self) -> "AsyncAdaptedConnection":
        # Raise on a closed cursor rather than returning the post-close proxy,
        # whose attribute access could surface a bare ReferenceError. Gate on
        # _closed, not the proxy type (which can be True on a live proxy).
        if self._closed:
            raise InterfaceError(f"cursor is closed (id={id(self)})")
        return self._adapt_connection

    # Stub raises (not absent) so a hard getattr gets a dbapi.Error, not a bare
    # AttributeError. The underlying AsyncCursor.rownumber is a real counter;
    # the adapter declines to mirror it to avoid per-fetch increment sites.
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
        # Validate mode before NotSupportedError so a typo surfaces as caller bug.
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
        raise NotSupportedError("dqlite cursors are not scrollable")

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.close()

    def __reduce__(self) -> NoReturn:
        # Reject pickling: the back-referenced AsyncConnection is loop-bound and
        # holds a live socket. Name the adapter class for a layer-correct error.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — back-"
            f"references a loop-bound dbapi AsyncConnection holding a "
            f"live socket and asyncio.Lock; reconstruct from the "
            f"engine in the target process instead."
        )


class AsyncAdaptedConnection(AdaptedConnection):
    """Adapts an AsyncConnection for SQLAlchemy's greenlet-based async engine,
    bridging sync-looking methods to the async connection via await_only().

    No ``_execute_mutex`` (unlike SA's reference connector): this adapter keeps
    no long-lived cursor, and the dbapi ``AsyncConnection`` op-lock already
    serialises commit/execute/rollback at the connection layer.
    """

    # dbapi in __slots__ mirrors SA's reference connector; third-party
    # instrumentation introspects ``dbapi_connection.dbapi`` for exception
    # classes. Without our own __slots__ each instance gets a __dict__.
    __slots__ = ("dbapi",)

    # SA convention: expose await_ as a staticmethod for external
    # instrumentation. Internal sites call module-level await_only directly so
    # test fixtures that monkeypatch it keep working.
    await_ = staticmethod(await_only)

    # Class-level cursor-class hooks let dialect subclasses swap the cursor
    # without overriding cursor(). _ss_cursor_cls is aliased for introspection
    # parity only — server-side cursors are pinned off.
    _cursor_cls: ClassVar[type] = AsyncAdaptedCursor
    _ss_cursor_cls: ClassVar[type] = AsyncAdaptedCursor

    @staticmethod
    def _terminate_handled_exceptions() -> tuple[type[BaseException], ...]:
        """Introspection-parity hook for SA async tooling; the tuple must mirror
        :meth:`terminate`'s catch arms (transport-class + RuntimeError + cancel).
        """
        return _TRANSPORT_CLASS_EXCEPTIONS + (RuntimeError, asyncio.CancelledError)

    def __init__(
        self,
        dbapi: Any,
        connection: "AsyncConnection | None" = _UNSET,
    ) -> None:
        # Signature ``(dbapi, connection)`` mirrors SA's reference connector so
        # third-party instrumentation constructs adapters the same way.
        # _UNSET (not None) discriminates the legacy single-positional
        # ``(connection,)`` shape from an explicit ``connection=None``.
        if connection is _UNSET:
            inner_conn: Any = dbapi
            dbapi_module: Any = None
        else:
            inner_conn = connection
            dbapi_module = dbapi
        # Detect the positional-swap call the _UNSET sentinel can't catch:
        # first arg shape-looks like a connection (cursor, no OperationalError)
        # and second like a dbapi module. Both firing = unambiguous swap.
        if (
            dbapi_module is not None
            and inner_conn is not None
            and not hasattr(dbapi_module, "OperationalError")
            and hasattr(dbapi_module, "cursor")
            and not hasattr(inner_conn, "cursor")
            and hasattr(inner_conn, "OperationalError")
        ):
            raise TypeError(
                f"AsyncAdaptedConnection __init__: positional "
                f"arguments appear swapped — first positional "
                f"({type(dbapi_module).__name__}) has 'cursor' but "
                f"no 'OperationalError'; second positional "
                f"({type(inner_conn).__name__}) has "
                f"'OperationalError' but no 'cursor'. Reference "
                f"shape: AsyncAdaptedConnection(dbapi, connection)."
            )
        self._connection: Any = inner_conn
        self.dbapi = dbapi_module

    def __reduce__(self) -> NoReturn:
        # Reject pickling: wraps a loop-bound AsyncConnection. Name the adapter
        # class for a layer-correct error rather than the inner dbapi class.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — wraps a "
            f"loop-bound dbapi AsyncConnection holding a live socket "
            f"and asyncio.Lock; reconstruct from the engine in the "
            f"target process instead."
        )

    @property
    def driver_connection(self) -> Any:
        # Closed-state guard: raise InterfaceError rather than let the post-close
        # weakref.proxy surface a bare ReferenceError to SA connect callbacks.
        if type(self._connection) in weakref.ProxyTypes:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        return self._connection

    def run_async(self, fn: Any) -> Any:
        # Closed-state guard — see driver_connection.
        if type(self._connection) in weakref.ProxyTypes:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        return super().run_async(fn)

    def cursor(self, server_side: bool = False) -> AsyncAdaptedCursor:
        # Closed-state check FIRST (before server_side) so a closed adapter
        # surfaces the actionable InterfaceError, not NotSupportedError. Guards
        # against the post-close proxy raising a bare ReferenceError downstream.
        if type(self._connection) in weakref.ProxyTypes:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if server_side:
            raise NotSupportedError(
                "Server-side cursors are not supported by the dqlite dialect; "
                "supports_server_side_cursors is pinned to False."
            )
        # Via the class-level hook so subclasses can swap the cursor class.
        cursor: AsyncAdaptedCursor = self._cursor_cls(self)
        return cursor

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> AsyncAdaptedCursor:
        """SA-reference parity convenience: open a cursor, execute, return it.
        SA-internal paths call ``dbapi_connection.execute(...)`` directly.
        """
        # One try-frame routes both cursor() and execute() faults through
        # _handle_exception; the outer frame closes any opened cursor on raise.
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
        # The only level dqlite honours (Raft consensus). Read-only; exposed so
        # SA/middleware probes don't see None. The setter side is short-circuited
        # by SA's engine flow.
        return "SERIALIZABLE"

    @property
    def autocommit(self) -> bool:
        # Report False: SA manages BEGIN/COMMIT at this layer even though the
        # underlying wire is autocommit-by-default. Exposed for SA/middleware
        # probes that would otherwise see None.
        return False

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        # Reject a direct ``conn.autocommit = True`` (which would bypass SA's
        # engine-level AUTOCOMMIT guard); accept False as a no-op.
        if value:
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)

    def _handle_exception(self, error: BaseException) -> NoReturn:
        """Centralised exception-normalisation hook for commit/rollback/execute.

        Accepts ``BaseException`` (wider than SA's reference ``Exception``) so the
        cursor-level ``except BaseException`` catch sites can route through here
        while their finally closes the underlying cursor; non-loop-state shapes
        (including KI/SystemExit/cancel) fall through to ``raise error``.
        """
        # PEP 654 cancel split runs BEFORE the loop-state remap so a mixed group
        # (cancel + loop-state RuntimeError from a cross-loop TaskGroup)
        # propagates the cancel instead of firing the remap on the RuntimeError.
        if isinstance(error, BaseExceptionGroup):
            cancel_group, remainder = error.split(
                lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
            )
            if cancel_group is not None:
                # ``from None`` so the cancel isn't weighted by the group on
                # __context__. The discarded loop-state remainder self-heals:
                # the next acquire re-fires it without a sibling cancel and the
                # remap then invalidates the slot (one wasted retry, no poison).
                raise cancel_group from None
            # ``if`` not ``assert`` so -O doesn't skip it; unreachable per split.
            if remainder is None:
                raise error
            _remap_loop_state_runtime_error(remainder)
            # Group-of-one (a TaskGroup wrapping a single SQL op): unwrap so the
            # original class identity reaches user ``except IntegrityError`` etc.
            # rather than being flattened into an aggregate OperationalError.
            if len(remainder.exceptions) == 1:
                child = remainder.exceptions[0]
                raise child from remainder
            child_classes = {type(c).__name__ for c in remainder.exceptions}
            raise OperationalError(
                f"aggregate {type(remainder).__name__} with "
                f"{len(remainder.exceptions)} child(ren) "
                f"of class(es) {sorted(child_classes)}",
                code=None,
            ) from remainder
        _remap_loop_state_runtime_error(error)
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
        # Idempotency short-circuit: post-close the inner conn is a weakref.proxy
        # whose attribute access would raise ReferenceError on a second close.
        if type(self._connection) in weakref.ProxyTypes:
            return

        # Outside a greenlet (GC sweep / atexit), skip rollback + async close and
        # reap synchronously — await_only would only allocate a MissingGreenlet.
        if not in_greenlet():
            try:
                self._force_close_transport()
            finally:
                self._release_inner_strong_ref()
            return

        # Outer try/finally guarantees the proxy swap on EVERY exit arm so a
        # closed adapter never pins the inner AsyncConnection.
        try:
            # Best-effort rollback before close (no-op if no txn active). Narrow
            # suppression to transport-class so programming bugs still propagate;
            # inner try/finally runs close() regardless of how rollback exits.
            try:
                try:
                    await_only(self._connection.rollback())
                except _TRANSPORT_CLASS_EXCEPTIONS as exc:
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
                    # Lowercase once (CPython phrasing isn't a stable API);
                    # if/elif/else makes mutual exclusion structural rather than
                    # relying on _handle_exception's unenforced NoReturn.
                    msg_lower = str(exc).lower()
                    if "different loop" in msg_lower or "different event loop" in msg_lower:
                        self._handle_exception(exc)  # NoReturn
                    # "Event loop is closed" during dispose after a per-call
                    # asyncio.run() tore the loop down; has_terminate promise =
                    # don't propagate, so debug-log and return (proxy swap still
                    # runs via the outer finally).
                    elif "event loop is closed" in msg_lower:
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
                    # Nested-loop "already running"; route through the same
                    # remap as _handle_exception / base.py disconnect messages.
                    elif "loop is already running" in msg_lower:
                        self._handle_exception(exc)  # NoReturn
                    else:
                        raise
                # CancelledError from rollback propagates (the finally still
                # runs close, whose cancel arm force-closes before re-raising).
            finally:
                # Must-not-raise teardown: transport faults and a RuntimeError
                # (defunct/closed loop during dispose) are suppressed; other bugs
                # propagate.
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
                    # Defunct-loop close during dispose; reap synchronously and
                    # stay quiet (has_terminate promise = don't propagate).
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
                    # Force-close the writer synchronously, then re-raise so the
                    # cancel still propagates (proxy swap runs via outer finally).
                    self._force_close_transport()
                    raise
        finally:
            self._release_inner_strong_ref()

    def _release_inner_strong_ref(self) -> None:
        """Swap ``self._connection`` for a ``weakref.proxy``; shared by close()
        and terminate(). Suppress TypeError (non-weakref-able test doubles) and
        ReferenceError (double-call after the inner was GC'd).
        """
        with contextlib.suppress(TypeError, ReferenceError):
            self._connection = weakref.proxy(self._connection)

    def _force_close_transport(self) -> None:
        """Best-effort synchronous transport teardown, bypassing the async close
        machinery. Used on non-greenlet finalize paths (GC sweep) where
        await_only would raise MissingGreenlet.

        Idempotent. Absorbs Exception (missing hook, dead-proxy ReferenceError,
        writer.close failure) but NOT CancelledError / KI / SystemExit — the
        cancel must reach the parent TaskGroup; cancel-path callers catch and
        re-raise around this helper.
        """
        peer: object | None = None
        try:
            peer = _log_safe_peer(self._connection)
            hook = getattr(self._connection, "force_close_transport", None)
            if hook is None:
                # Older dbapi without the hook; log the no-op for the audit trail.
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

    def force_close_transport(self) -> None:
        """Public alias of :meth:`_force_close_transport` (the name the inherited
        ``DqliteDialect.do_close`` transport-class fallback reaches for), plus the
        ``_release_inner_strong_ref`` swap so that fallback path also releases the
        inner conn. Swap-tail Exception suppressed so cleanup can't re-raise.
        """
        try:
            self._force_close_transport()
        finally:
            with contextlib.suppress(Exception):
                self._release_inner_strong_ref()

    def terminate(self) -> None:
        """Force-close without rollback. SA's pool calls this (via
        ``do_terminate``) when ``has_terminate=True`` and a connection must be
        forcibly reclaimed. Swaps in the post-close proxy on every exit arm.
        """
        # Idempotency short-circuit — see close(); pool can race a parallel close.
        if type(self._connection) in weakref.ProxyTypes:
            return

        # Non-greenlet finalize reaps synchronously — see close().
        if not in_greenlet():
            try:
                self._force_close_transport()
            finally:
                self._release_inner_strong_ref()
            return

        # has_terminate=True promises a non-blocking path; suppress transport
        # failures, force-close synchronously on cancel. Outer try/finally runs
        # the proxy swap on every arm.
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
                # Defunct-loop close during dispose; reap synchronously, stay
                # quiet (has_terminate promise = don't propagate).
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
                # See close() — force-close synchronously, then re-raise.
                self._force_close_transport()
                raise
        finally:
            self._release_inner_strong_ref()


class DqliteDialect_aio(DqliteDialect):
    """Async SQLAlchemy dialect for dqlite.

    Use with SQLAlchemy's async engine:
        create_async_engine("dqlite+aio://host:port/database")
    """

    # Matches the entry-point short name so dialect_description renders the
    # canonical "dqlite+aio" the user types into the URL.
    driver = "aio"
    is_async = True
    # MUST be redeclared (not inherited): SA reads it via a single-class
    # __dict__ lookup, not MRO. Removing this silently disables statement cache.
    supports_statement_cache = True

    # Pinned False locally to defend against a base-class default flip — the
    # adapter has no SS-cursor code path (dqlite has no server-side cursors).
    supports_server_side_cursors = False

    # Pinned True locally (defends against an MRO flip to the DefaultDialect
    # False): AsyncAdaptedConnection provides terminate(), which the pool's
    # forced-disposal path requires.
    has_terminate = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return AsyncAdaptedQueuePool

    def do_terminate(self, dbapi_connection: Any) -> None:
        """Pool forced-disposal hook; defers to ``terminate()`` (no pre-close
        rollback). has_terminate=True promises a non-raising path, so both arms
        absorb. CancelledError is NOT caught (must propagate). Two-tier catch:
        expected transport shapes DEBUG-log; anything else WARNING-logs as a
        likely dbapi-refactor regression.
        """
        _do_terminate_logging("terminate", dbapi_connection, dbapi_connection.terminate, logger)

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Bespoke async ping: run SELECT 1 directly through the dbapi cursor in
        one await_only hop (vs three through the sync inherited path), routing
        loop-state RuntimeError through _handle_exception so SA evicts the slot.
        Codes 11/24/26 still classify as ping-fail.
        """
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
            # Any RuntimeError on the ping path is slot-fatal (SA's
            # _do_ping_w_event catches only dbapi.Error and wouldn't evict).
            return False
        return True

    async def _async_ping(self, dbapi_connection: Any) -> None:
        """Async leg of do_ping: open cursor, run SELECT 1, close.

        Only RuntimeError is remapped via _handle_exception; dbapi.Error
        subclasses propagate to do_ping's outer catch. DO NOT narrow that catch
        without widening this except to (RuntimeError, ProgrammingError).
        """
        # Closed-state guard — raise InterfaceError rather than let the post-close
        # proxy surface a bare ReferenceError past do_ping's classifier.
        if type(dbapi_connection._connection) in weakref.ProxyTypes:
            logger.debug(
                "_async_ping: adapter already closed (id=%s); reporting "
                "ping failure so SA pool retires the slot. Likely a "
                "sibling task closed the adapter between pool checkout "
                "and ping (GC sweep, engine.dispose race, parallel "
                "task close).",
                id(dbapi_connection),
            )
            raise InterfaceError(f"Connection is closed (id={id(dbapi_connection)})")
        try:
            cur = dbapi_connection._connection.cursor()
            try:
                # Execute alone proves the round-trip (matches the sync sibling);
                # the row arrives in the execute response, so no fetch is needed.
                await cur.execute(self._dialect_specific_select_one)
            finally:
                # Suppress transport-class + cancel on close (the ping already
                # ran), but DEBUG-log so a flapping leader stays observable;
                # programmer-bug shapes propagate.
                peer = _log_safe_peer(dbapi_connection._connection)
                try:
                    cur.close()
                except (
                    DatabaseError,
                    InterfaceError,
                    DqliteConnectionError,
                    OSError,
                    asyncio.CancelledError,
                ) as exc:
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
        # Fast-path: a closed adapter (inner conn is a weakref.proxy) is
        # definitionally disconnected. Otherwise fall through to the base
        # type/code/substring classifier.
        if (
            connection is not None
            and isinstance(connection, AsyncAdaptedConnection)
            and type(connection._connection) in weakref.ProxyTypes
        ):
            return True
        return super().is_disconnect(e, connection, cursor)

    @classmethod
    def import_dbapi(cls) -> types.ModuleType:
        # Returns dqlitedbapi.aio (the async submodule), NOT the top-level module
        # the sync dialect imports — the asymmetry is deliberate.
        from dqlitedbapi import aio

        return aio

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Validate connect kwargs, eagerly open the TCP connection, and wrap it.

        On eager-connect failure, force-close the half-built raw_conn via a
        temporary adapter's terminate() (no graceful close — the handshake never
        completed and could re-raise a torn-down-loop RuntimeError).

        ``async_creator_fn`` (SA convention) injects a custom factory; popped
        before validation so the allowlist doesn't reject it. We always await
        ``raw_conn.connect()``, so a custom creator's connect() must be an
        idempotent coroutine (the dbapi's own is).
        """
        creator_fn = cparams.pop("async_creator_fn", None)
        # An ``async def`` creator would return a coroutine from the synchronous
        # call, failing far from the config site; reject up front. partial-around-
        # async-def detection relies on CPython 3.12+ (requires-python >=3.13).
        if creator_fn is not None and not callable(creator_fn):
            raise ArgumentError(
                f"async_creator_fn must be callable; got "
                f"{type(creator_fn).__name__}. The SA dialect calls the "
                f"creator synchronously to obtain an AsyncConnection-shape "
                f"object whose .connect() coroutine is then awaited."
            )
        if creator_fn is not None and inspect.iscoroutinefunction(creator_fn):
            raise ArgumentError(
                "async_creator_fn must be a regular (sync) callable that "
                "returns an AsyncConnection-shape object exposing a "
                "coroutine-shaped .connect() method. Got an async def "
                "(or async-coroutine wrapper); the SA dialect calls the "
                "creator synchronously and then awaits .connect() on the "
                "returned object. See the contract in DqliteDialect_aio."
                "connect's docstring."
            )
        self._validate_connect_kwargs(cparams)
        # Construct inside the try frame so a BaseException between assignment and
        # try: can't leak the freshly-built AsyncConnection without cleanup.
        raw_conn: Any = None
        try:
            if creator_fn is not None:
                raw_conn = creator_fn(*cargs, **cparams)
            else:
                raw_conn = self.loaded_dbapi.connect(*cargs, **cparams)
            try:
                await_only(raw_conn.connect())
            except BaseException as error:
                # Route eager-connect loop-state RuntimeErrors through the same
                # remap as every other await_only site so the pool evicts.
                _remap_loop_state_runtime_error(error)
                raise  # unreachable when remap matches; preserved otherwise
        except BaseException:
            if raw_conn is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    AsyncAdaptedConnection(self.loaded_dbapi, raw_conn).terminate()
            raise
        return AsyncAdaptedConnection(self.loaded_dbapi, raw_conn)

    def get_driver_connection(self, dbapi_connection: Any) -> Any:
        # Closed-state guard — raise InterfaceError on the post-close proxy
        # (see driver_connection). ``type(inner) in ProxyTypes``, not isinstance:
        # isinstance on a dead proxy can itself raise ReferenceError.
        inner = dbapi_connection._connection
        if type(inner) in weakref.ProxyTypes:
            raise InterfaceError(f"Connection is closed (id={id(dbapi_connection)})")
        return inner
