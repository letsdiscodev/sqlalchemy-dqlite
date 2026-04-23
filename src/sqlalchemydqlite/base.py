"""Base dqlite dialect for SQLAlchemy."""

import datetime
import logging
import math
import types
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from dqlitewire import LEADER_ERROR_CODES as _LEADER_CHANGE_CODES

logger = logging.getLogger(__name__)

__all__ = ["DqliteDialect"]

_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})


def _walk_cause_chain(e: BaseException, max_depth: int = 10) -> Iterator[BaseException]:
    """Yield ``e`` and each ``__cause__`` / ``__context__`` up to ``max_depth``.

    The ``visited`` set prevents an infinite loop on pathological
    cycles (``raise X from X`` or a deeply-nested wrap that loops
    back). The depth cap is a second line of defence so a truly
    degenerate chain cannot drag classifier latency even if the
    visited-set catch misses for some reason. Same shape as
    ``traceback._format_final_exc_line``'s own chain traversal.

    A single-hop ``__cause__`` check would miss any wrap tower taller
    than one — retry decorators, telemetry middleware, and circuit
    breakers layered above the client can push the real
    ``DqliteConnectionError`` / ``ClusterError`` two or more hops
    away from the exception SA hands to ``is_disconnect``. Walking
    the full chain keeps the type-dispatch robust against those
    layerings.
    """
    seen: set[int] = set()
    cur: BaseException | None = e
    depth = 0
    while cur is not None and id(cur) not in seen and depth < max_depth:
        seen.add(id(cur))
        yield cur
        cur = cur.__cause__ or cur.__context__
        depth += 1


def _parse_url_bool(key: str, raw: str) -> bool:
    """Strict bool parser for URL query parameters.

    Accepts the conventional truthy/falsy token sets and raises
    ``ArgumentError`` on anything else so a typo like ``?flag=flase``
    surfaces instead of silently coercing to False.
    """
    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ArgumentError(
        f"Invalid bool value for URL parameter {key!r}: {raw!r} "
        f"(accepted: 1/0, true/false, yes/no, on/off)"
    )


class _DqliteDateTime(sqltypes.DateTime):
    """DateTime processor honouring the ``timezone`` declaration.

    ``dqlitedbapi`` decodes ISO8601-tagged cells as ``datetime.datetime``
    (tz matching the wire value), and UNIXTIME-tagged cells as
    UTC-aware ``datetime.datetime`` (via ``fromtimestamp(..., tz=UTC)``).
    SQLAlchemy's ``DateTime(timezone=False)`` contract demands naive
    values; when an expression like ``unixepoch(col)`` or a column with
    UNIXTIME affinity lands in such a field, the raw UTC-aware
    datetime would leak through without an override.

    Also accept ``str`` cells (affinity-stripped by an expression such
    as ``strftime('%Y-%m-%d %H:%M:%S', col)``), parsing them via
    ``datetime.fromisoformat`` so the ORM field always sees a
    ``datetime.datetime``.

    When ``fromisoformat`` raises ``ValueError`` on a TEXT-tagged cell
    (malformed row inserted by another writer, legacy non-ISO8601
    format, etc.), log a WARNING and pass the raw ``str`` through. The
    legacy silent pass-through violated the column-type contract without
    any observability; a log line makes the data-integrity problem
    visible while keeping the forgiving behaviour that lets operators
    repair bad rows at their own pace rather than aborting a full read.
    """

    def bind_processor(self, dialect: Any) -> None:
        return None

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        want_timezone = self.timezone

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, str):
                # Affinity-stripped cell: TEXT-tagged on the wire, so
                # dqlitedbapi did not run its datetime converter.
                try:
                    value = datetime.datetime.fromisoformat(value)
                except ValueError as e:
                    logger.warning(
                        "DateTime processor received unparseable ISO8601 string %r: %s",
                        value,
                        e,
                    )
                    return value
            if isinstance(value, datetime.datetime):
                if want_timezone:
                    return value
                # DateTime(timezone=False): strip the UTC tz the
                # UNIXTIME decoder attached so the ORM field sees a
                # naive wall-clock (interpreted as UTC).
                if value.tzinfo is not None:
                    return value.replace(tzinfo=None)
                return value
            return value

        return process


class _DqliteDate(sqltypes.Date):
    """Date processor handling datetime and ISO8601-string inputs.

    ``dqlitedbapi`` returns ``datetime.datetime`` for DATE columns
    (C server tags DATETIME / DATE / TIMESTAMP as ``DQLITE_ISO8601``)
    — narrow to ``datetime.date``. Also accept ``str`` for columns
    whose affinity was stripped by a SQL expression (e.g.
    ``func.date(col)``), parsing via ``datetime.date.fromisoformat``.

    A tz-aware input datetime has its tzinfo silently dropped by
    ``.date()`` (``datetime.date`` has no tz support). The returned
    date is the UTC-day portion when the dbapi decoded an ISO8601
    value — not the viewer's local day. Applications that care about
    local-day semantics should store DATETIME instead and do the
    narrowing themselves.

    When ``fromisoformat`` raises ``ValueError`` on a TEXT-tagged cell,
    log a WARNING and pass the raw ``str`` through. Same rationale as
    :class:`_DqliteDateTime` — a silent type-contract violation is the
    footgun; a log line surfaces the bad data without aborting the
    full read.
    """

    def bind_processor(self, dialect: Any) -> None:
        return None

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                # Deliberate: tzinfo is dropped. See class docstring.
                return value.date()
            if isinstance(value, str):
                try:
                    return datetime.date.fromisoformat(value)
                except ValueError as e:
                    logger.warning(
                        "Date processor received unparseable ISO8601 string %r: %s",
                        value,
                        e,
                    )
                    return value
            return value

        return process


class DqliteDialect(SQLiteDialect):
    """SQLAlchemy dialect for dqlite.

    Inherits from SQLite dialect since dqlite is compatible with SQLite.
    """

    name = "dqlite"
    driver = "dqlitedbapi"

    # dqlite uses qmark parameter style
    paramstyle = "qmark"

    # Enable SQLAlchemy statement caching
    supports_statement_cache = True

    # dqlitedbapi returns native Python ``bytes`` for BLOB columns (the
    # wire codec emits ``bytes`` for ``ValueType.BLOB`` on the result
    # path). Pin True locally so ``LargeBinary.result_processor`` can
    # skip the redundant ``bytes(value)`` wrap on every BLOB cell.
    returns_native_bytes = True

    # dqlite's wire protocol has a first-class BOOLEAN tag
    # (``ValueType.BOOLEAN = 11``); the server returns native Python
    # booleans for columns tagged BOOLEAN and dqlitedbapi passes them
    # through unchanged. Unlike the inherited pysqlite dialect
    # (``supports_native_boolean = False``), we don't need SQLAlchemy
    # to emit a ``CHECK (col IN (0, 1))`` constraint — the wire
    # contract enforces the 0/1 invariant.
    supports_native_boolean = True
    # SQLAlchemy's Boolean type compiler gates
    # ``non_native_boolean_check_constraint`` behind
    # ``supports_native_boolean``, so the flag is functionally inert
    # for us today. Pin False anyway to document intent and to keep
    # the pin in lockstep with ``supports_native_boolean`` if a future
    # SQLAlchemy release decouples the two.
    non_native_boolean_check_constraint = False

    # Since isolation is always SERIALIZABLE and cannot be weakened, the
    # reported isolation level is trustworthy across transactions. SQLAlchemy
    # skips defensive isolation-level resets when this is True.
    supports_sane_isolation_level = True

    # dqlite/SQLite have no native DECIMAL type — values are stored as TEXT
    # or REAL. Declare explicitly to document the contract (matches pysqlite
    # inherited default, pinned here against upstream drift).
    supports_native_decimal = False

    # dqlite runs SQLite >= 3.35, which supports the RETURNING clause on
    # INSERT / UPDATE / DELETE. Pin locally so upstream changes to
    # SQLiteDialect's RETURNING detection (e.g. version-gated discovery)
    # can't silently change dqlite behaviour. All three of SQLAlchemy 2.x's
    # tripartite RETURNING flags default to True on the parent class today.
    # Same reasoning applies to the multi-FROM RETURNING variant.
    insert_returning = True
    update_returning = True
    delete_returning = True
    update_returning_multifrom = True

    # Executemany-RETURNING flags. dqlitedbapi's executemany accumulates
    # per-parameter-set RETURNING rows via its _ExecuteManyAccumulator
    # so all three DML kinds can deliver the full row set in a single
    # call. The INSERT flag is a memoized property on DefaultDialect
    # (derived from ``insert_returning and use_insertmanyvalues``) — pin
    # explicitly so upstream drift can't silently flip it. UPDATE /
    # DELETE flags default to False on DefaultDialect, which blocks
    # SQLAlchemy from issuing executemany RETURNING even though the
    # wire path supports it; pin True to surface the capability.
    # Integration-verified in tests/integration/test_bulk_dml_returning.py.
    insert_executemany_returning = True
    update_executemany_returning = True
    delete_executemany_returning = True

    # SQLite >= 3.7.11 supports multi-row INSERT VALUES, which SQLAlchemy's
    # insertmanyvalues optimisation depends on. Pin the flag so bulk-insert
    # behaviour stays stable against upstream dialect drift.
    supports_multivalues_insert = True

    # Rowcount truthfulness flags. SQLite (and therefore dqlite) reports
    # accurate UPDATE / DELETE rowcounts and accurate aggregated
    # executemany rowcounts. The two ``*_returning`` flags are False
    # because SQLAlchemy's insertmanyvalues-with-RETURNING path relies
    # on separate accounting; pinning False matches the inherited
    # SQLiteDialect behaviour but, like the other pins above, guards
    # against silent upstream drift.
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_sane_rowcount_returning = False
    supports_sane_multi_rowcount_returning = False

    # Insert-path flags inherited from SQLiteDialect. SQLAlchemy's
    # insertmanyvalues codegen, DEFAULT VALUES form, and rowid handling
    # all key on these. Pin locally for the same "against upstream
    # drift" reason as the RETURNING trio above — a version-gated
    # change in a future SQLAlchemy release would silently alter
    # dqlite's insert behaviour otherwise.
    use_insertmanyvalues = True
    supports_default_metavalue = True
    supports_default_values = True
    insert_null_pk_still_autoincrements = True

    # Override the SQLite dialect's string-based DATE/DATETIME processors:
    # dqlitedbapi returns datetime objects (PEP 249), not ISO strings.
    colspecs = {
        **SQLiteDialect.colspecs,
        sqltypes.DateTime: _DqliteDateTime,
        sqltypes.Date: _DqliteDate,
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # ``SQLiteDialect.__init__`` writes *instance* attributes based
        # on ``self.dbapi.sqlite_version_info`` and ``util.pypy``:
        #
        #   if self.dbapi.sqlite_version_info < (3, 35) or util.pypy:
        #       self.insert_returning = ... = False
        #
        # Instance attrs shadow the class-level pins above. On CPython
        # with dqlitedbapi's pinned sqlite_version_info the overrides
        # happen to match our pins, but on PyPy the ``or util.pypy``
        # branch unconditionally zeroes RETURNING — silently breaking
        # every RETURNING-based code path on PyPy. Re-apply the pins
        # at the instance level after the parent's ``__init__`` runs.
        #
        # ``supports_default_values`` and ``supports_multivalues_insert``
        # are also written by the parent from version checks; re-pin
        # them here for the same reason.
        self.insert_returning = True
        self.update_returning = True
        self.delete_returning = True
        self.supports_default_values = True
        self.supports_multivalues_insert = True

        # ``SQLiteDialect.__init__`` also version-gates
        # ``insertmanyvalues_max_parameters``:
        #
        #   if self.dbapi.sqlite_version_info < (3, 32, 0):
        #       self.insertmanyvalues_max_parameters = 999
        #
        # dqlitedbapi's pinned ``sqlite_version_info = (3, 35, 0)``
        # sidesteps the gate today, but match the drift-defense
        # discipline above: if the parent ever extends the block with
        # ``or util.pypy`` (symmetric to the RETURNING gate) a
        # PyPy-hosted engine would silently cap batch INSERTs at 999
        # parameters — a surprise performance regression. Re-apply
        # DefaultDialect's value literally (SA 2.x: 32700) so the
        # instance-level pin is independent of the parent's version
        # check.
        self.insertmanyvalues_max_parameters = 32700

    @classmethod
    def import_dbapi(cls) -> types.ModuleType:
        import dqlitedbapi

        return dqlitedbapi

    # Whitelist of URL query parameters we forward to the DBAPI connect
    # call. Unknown keys raise ``ArgumentError`` so typos surface.
    # Each entry pairs a string-to-value converter with an optional
    # predicate that runs after coercion to catch semantic out-of-range
    # values (zero, negative, NaN, inf). The predicate may be ``None`` for
    # bool knobs that don't admit a range check.
    # ``trust_server_heartbeat`` uses a URL-friendly bool parser because
    # bool("False") evaluates truthy (non-empty string). Unknown tokens
    # raise ``ArgumentError`` to prevent a typo from silently disabling
    # the opt-in.
    # Defense-in-depth upper bounds on row / frame governors prevent
    # a misconfigured URL (``?max_total_rows=9999999999999999``) from
    # silently disabling the ceiling set by ISSUE-98 / ISSUE-123. The
    # bounds are pragmatic: 2**31-1 rows and 10x the default frame
    # cap leave plenty of headroom for real workloads while refusing
    # values only a typo would supply.
    _URL_QUERY_ALLOWED: dict[str, tuple[Callable[[str], Any], Callable[[Any], bool] | None]] = {
        "timeout": (float, lambda v: math.isfinite(v) and v > 0),
        "max_total_rows": (int, lambda v: 0 < v <= 2**31 - 1),
        "max_continuation_frames": (int, lambda v: 0 < v <= 1_000_000),
        "trust_server_heartbeat": (
            lambda s: _parse_url_bool("trust_server_heartbeat", s),
            None,
        ),
        "close_timeout": (float, lambda v: math.isfinite(v) and v > 0),
    }

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from URL.

        URL format: ``dqlite://host:port/database?timeout=...``

        Known query parameters are typed and range-validated at URL-parse
        time so typos (``?timeoutt=5``), unparseable types
        (``?timeout=abc``), and out-of-range values
        (``?max_total_rows=-1``) all raise :class:`ArgumentError` before
        any pool is built.
        """
        if url.username or url.password:
            # dqlite has no built-in authentication; credentials
            # embedded in the URL would be silently dropped. Reject
            # at parse time with a clear message — matches pysqlite's
            # create_connect_args policy.
            raise ArgumentError(
                "Invalid URL: dqlite does not accept username or password in the URL"
            )
        host = url.host or "localhost"
        if url.port is not None and not (1 <= url.port <= 65535):
            # SQLAlchemy's URL parser normally rejects non-integer ports
            # but will happily carry an integer outside the legal TCP
            # range if the URL was constructed via ``URL.create(port=…)``.
            # Catch here so typos fail at URL-parse time, matching the
            # validation style used for known query parameters.
            raise ArgumentError(f"dqlite URL port {url.port!r} is out of the valid 1..65535 range")
        port = url.port or 9001
        database = url.database or "default"

        address = f"{host}:{port}"
        kwargs: dict[str, Any] = {"address": address, "database": database}

        query = dict(url.query) if url.query else {}
        for key, raw in query.items():
            if key not in self._URL_QUERY_ALLOWED:
                raise ArgumentError(
                    f"Unknown dqlite URL query parameter {key!r}. "
                    f"Allowed: {sorted(self._URL_QUERY_ALLOWED)}"
                )
            converter, validator = self._URL_QUERY_ALLOWED[key]
            # URL query values can be str or tuple[str, ...] (when a key
            # appears multiple times). Take the last occurrence.
            raw_str = raw[-1] if isinstance(raw, tuple) else raw
            try:
                value = converter(raw_str)
            except (TypeError, ValueError) as e:
                raise ArgumentError(
                    f"Cannot convert URL query {key}={raw!r} to "
                    f"{getattr(converter, '__name__', 'expected type')}: {e}"
                ) from e
            if validator is not None and not validator(value):
                raise ArgumentError(f"URL query {key}={raw_str!r} is out of range")
            kwargs[key] = value

        return [], kwargs

    def get_isolation_level_values(
        self, dbapi_connection: DBAPIConnection
    ) -> Sequence[IsolationLevel]:
        """Return the isolation levels dqlite accepts.

        The parent ``SQLiteDialect`` advertises ``["READ UNCOMMITTED",
        "SERIALIZABLE"]`` because stdlib sqlite3 implements
        ``READ UNCOMMITTED`` via ``PRAGMA read_uncommitted`` in
        shared-cache mode. dqlite runs every statement through Raft
        consensus and has no mechanism to weaken isolation, so advertise
        only what we can honour — ``set_isolation_level`` below rejects
        anything else explicitly.
        """
        return ["SERIALIZABLE"]

    def get_isolation_level(self, dbapi_connection: DBAPIConnection) -> IsolationLevel:
        """Return the isolation level.

        dqlite doesn't support PRAGMA read_uncommitted, so we return
        SERIALIZABLE as the default isolation level.
        """
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection: DBAPIConnection, level: str | None) -> None:
        """Set isolation level.

        dqlite only supports SERIALIZABLE. Any other level is rejected
        with ``ArgumentError`` — including the AUTOCOMMIT sentinel and
        unknown strings like ``"READ COMMITTED"`` — because silently
        dropping the request would cause callers to lose either
        transactionality (AUTOCOMMIT) or the specific weaker-isolation
        semantics they asked for (READ COMMITTED etc.), neither of which
        is safe to fake.

        Note on reachability: SA's engine flow
        (``engine/default.py::_assert_and_set_isolation_level``) calls
        ``get_isolation_level_values()`` first and rejects unknown
        values with ``ArgumentError`` before reaching this method, so
        the branches below are largely defence-in-depth for third-party
        callers (test harnesses, custom engine implementations) that
        bypass SA's upstream validation.
        """
        if level is None or level == "SERIALIZABLE":
            return
        if level == "AUTOCOMMIT":
            raise ArgumentError(
                "dqlite does not support AUTOCOMMIT; every statement goes through "
                "Raft consensus and there is no per-statement autocommit mode. "
                "Use explicit commit() / rollback() on the connection."
            )
        raise ArgumentError(
            f"dqlite only supports SERIALIZABLE isolation; requested level "
            f"{level!r} is not supported."
        )

    # do_rollback / do_commit are intentionally left inherited from the
    # parent dialect. The "cannot commit/rollback — no transaction is
    # active" error is swallowed at the DBAPI layer (dqlitedbapi's
    # Connection.commit / rollback), so the dialect doesn't need its own
    # workaround. Matches stdlib sqlite3 semantics.

    _dqlite_disconnect_messages = (
        "Connection closed",
        "timed out",
        "Failed to connect",
        "not connected",
        "Not connected",
        # Wire-layer desync: ProtocolError / DecodeError / StreamError
        # in dqlitewire surface here. Paired with the client wrap at
        # ``dqliteclient/protocol.py`` which emits these prefixes, and
        # the dbapi wrap at ``cursor._call_client`` that now routes
        # ``client.ProtocolError`` to ``OperationalError`` (not
        # ``InterfaceError``) so the substring branch can see it.
        "Wire decode failed",
        "Wire stream error",
    )

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """Detect whether an exception indicates a broken connection.

        Prefer exception-type dispatch over message matching; the C
        server's error wording is not a contract. Type-based checks
        cover TCP resets, DNS failures, and partial-read timeouts that
        the hand-maintained substring list misses.
        """
        # Walk the full ``__cause__`` / ``__context__`` chain. The
        # dbapi's ``_call_client`` handler wraps
        # ``DqliteConnectionError`` / ``ClusterError`` into a bare
        # ``dbapi.OperationalError`` and chains the original via
        # ``raise ... from e``. A single-hop check would fail for any
        # additional wrap layer (retry decorator, telemetry middleware,
        # circuit breaker) between SA and the dbapi — the inner
        # transport-level cause sits two or more hops away. A bounded
        # visited-set walk picks up those layerings while staying
        # pathology-safe.
        #
        # ``ClusterError`` belongs in the disconnect set for the same
        # reason as ``DqliteConnectionError``: the slot is useless —
        # the cluster is mid-leader-blip or reporting "no reachable
        # leader" — and the pool must invalidate it. The policy-error
        # subclass ``ClusterPolicyError`` is deliberately excluded:
        # policy rejections are deterministic configuration errors,
        # and classifying them as disconnect would re-enter the pool's
        # retry loop against a permanent rejection. The order of the
        # subclass check matters — ``ClusterPolicyError`` inherits
        # from ``ClusterError`` so the policy branch must be checked
        # first to short-circuit.
        for cause in _walk_cause_chain(e):
            if isinstance(cause, _client_exc.ClusterPolicyError):
                # Policy rejection — never a disconnect. Stop walking;
                # any outer wrap was for classification purposes only.
                return False
            if isinstance(cause, (_client_exc.DqliteConnectionError, _client_exc.ClusterError)):
                return True
        # Underlying OS-level transport failures (socket RST, broken pipe,
        # DNS, connect refused, connection timeout). ``ConnectionError``,
        # ``BrokenPipeError``, and ``TimeoutError`` are all ``OSError``
        # subclasses, so a single ``OSError`` check covers every stdlib
        # transport-error shape (including ``ConnectionResetError`` /
        # ``ConnectionAbortedError`` / ``ConnectionRefusedError`` /
        # ``socket.gaierror`` that a narrower enumeration would miss).
        if isinstance(e, OSError):
            return True
        # ``dqlitedbapi.Connection`` / ``Cursor`` raise ``InterfaceError``
        # when operated on after ``close()``; match the narrow
        # "closed" substring so programming-error InterfaceErrors (e.g.
        # setinputsizes on a closed cursor) are NOT classified as
        # disconnect. The do_ping path already catches InterfaceError
        # for the same reason.
        if isinstance(e, _dbapi_exc.InterfaceError):
            message = str(e).lower()
            if "connection is closed" in message or "cursor is closed" in message:
                return True
        # Leader-change error codes signal that the connection is useless
        # even though it's TCP-alive.
        for err in (_dbapi_exc.OperationalError, _client_exc.OperationalError):
            if isinstance(e, err) and getattr(e, "code", None) in _LEADER_CHANGE_CODES:
                return True
        # Legacy substring fallback — kept so we still catch anything
        # that wasn't modelled as a specific exception type yet.
        if isinstance(e, _dbapi_exc.OperationalError):
            msg = str(e)
            for pattern in self._dqlite_disconnect_messages:
                if pattern in msg:
                    return True
        return super().is_disconnect(e, connection, cursor)

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Check if the connection is still alive.

        Only connection-level exceptions are interpreted as "dead"; any
        other exception propagates so the caller can see real bugs
        instead of having them silently rewritten as "please reconnect."
        """
        cursor = dbapi_connection.cursor()
        try:
            try:
                cursor.execute("SELECT 1")
                return True
            except (
                _dbapi_exc.OperationalError,
                _dbapi_exc.InterfaceError,
                _client_exc.DqliteConnectionError,
                OSError,
            ):
                return False
        finally:
            # Narrow the suppression to the same set as the outer
            # except: connection-level errors are expected on a dead
            # socket and must not crash the ping; programming bugs
            # (TypeError, AttributeError, ValueError, …) must still
            # propagate so refactors can't silently break the probe.
            # DEBUG-log the suppressed cause so operators can tell
            # close-swallow from close-success in logs.
            try:
                cursor.close()
            except (
                _dbapi_exc.OperationalError,
                _dbapi_exc.InterfaceError,
                _client_exc.DqliteConnectionError,
                OSError,
            ) as exc:
                logger.debug(
                    "do_ping: cursor.close failed (%s); proceeding",
                    type(exc).__name__,
                    exc_info=True,
                )

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return the server's SQLite version as a tuple.

        Forwards ``dqlitedbapi.sqlite_version_info`` (a module-level
        constant pinning the minimum supported SQLite version). Matches
        how pysqlite implements the same override — a one-liner that
        lets ``AttributeError`` propagate if the bound DBAPI module
        does not expose the constant.

        The earlier ``(3, 35, 0)`` fallback had the inverse failure
        mode of the pre-fix ``(3, 0, 0)`` fallback: instead of silently
        DISABLING RETURNING on a broken DBAPI stub, it silently ENABLED
        RETURNING / multi-values / insertmanyvalues against a driver
        that might not implement any of them — yielding cryptic runtime
        failures far from the real cause. Dropping the fallback so the
        config error surfaces at dialect-init time.
        """
        return tuple(self.dbapi.sqlite_version_info)  # type: ignore[union-attr]
