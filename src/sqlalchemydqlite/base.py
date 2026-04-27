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
from dqlitewire import SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB

# Primary SQLite codes that route to bare ``DatabaseError`` (rather
# than ``OperationalError`` or its subclasses) and that the dialect
# treats as slot-fatal — both during failure dispatch
# (``is_disconnect``'s substring scan) and during pre-ping
# (``do_ping``'s code-restricted ``DatabaseError`` arm). Hoisted from
# the two inline sets so a future addition (or removal) updates one
# place. ``do_ping``'s broader transport-class tuple intentionally
# stays separate (it includes ``ProgrammingError`` for cross-loop
# reuse, which is ping-specific and must not bleed into disconnect
# classification on real-query paths).
_BARE_DBE_DISCONNECT_CODES: frozenset[int] = frozenset(
    {SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB}
)

# Transport-class exception tuple for best-effort cleanup paths that
# must swallow a flaky close / rollback without aborting
# ``engine.dispose()``. Used by ``do_begin``'s post-BEGIN cursor
# close, and by the async adapter's ``close()`` rollback finally,
# ``close()`` close finally, and ``terminate()`` finally. Narrow on
# purpose: programmer-bug shapes (AttributeError, TypeError, bare
# RuntimeError) propagate so refactor regressions stay visible.
# ``do_ping`` keeps its own broader tuple — it includes
# ``ProgrammingError`` (cross-loop reuse) and the ``DatabaseError``
# umbrella (CORRUPT/FORMAT/NOTADB classification path), neither of
# which belongs on cleanup paths that run after a real query has
# already failed.
_TRANSPORT_CLASS_EXCEPTIONS: tuple[type[BaseException], ...] = (
    _dbapi_exc.OperationalError,
    _dbapi_exc.InterfaceError,
    _client_exc.DqliteConnectionError,
    OSError,
)

logger = logging.getLogger(__name__)

__all__ = ["DqliteDialect"]

_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})


def _walk_cause_chain(e: BaseException, max_depth: int = 25) -> Iterator[BaseException]:
    """Yield ``e`` and each ``__cause__`` / ``__context__`` /
    ``BaseExceptionGroup.exceptions`` child up to ``max_depth``.

    The ``visited`` set prevents an infinite loop on pathological
    cycles (``raise X from X`` or a deeply-nested wrap that loops
    back). The depth cap is a second line of defence so a truly
    degenerate chain cannot drag classifier latency even if the
    visited-set catch misses for some reason. Same shape as
    ``traceback._format_final_exc_line``'s own chain traversal,
    extended with PEP 654 ``ExceptionGroup`` children.

    A single-hop ``__cause__`` check would miss any wrap tower taller
    than one — retry decorators, telemetry middleware, and circuit
    breakers layered above the client can push the real
    ``DqliteConnectionError`` / ``ClusterError`` two or more hops
    away from the exception SA hands to ``is_disconnect``.

    The ``BaseExceptionGroup`` traversal is essential because the
    pool's ``initialize`` aggregates multiple connect failures via
    ``raise BaseExceptionGroup(..., [DqliteConnectionError(...), ...])``
    — without unwrapping the group's children, the disconnect
    classifier would never see the wrapped causes and the group
    would propagate as a non-disconnect error. The walk uses BFS
    over a queue so the depth budget is shared across cause /
    context hops AND group children fan-out.

    Group children enqueue at the parent's depth (not depth+1) because
    they are fan-out, not a wrap layer — preserving the spine-depth
    budget for cause/context hops which are the deep dimension. The
    visited-set still bounds total work so a pathologically nested
    group cannot loop. ``max_depth=25`` accommodates retry +
    telemetry + circuit-breaker + group-fanout towers of realistic
    depth without changing the cycle-defence contract.
    """
    from collections import deque

    seen: set[int] = set()
    queue: deque[tuple[BaseException, int]] = deque([(e, 0)])
    while queue:
        cur, depth = queue.popleft()
        if id(cur) in seen or depth >= max_depth:
            continue
        seen.add(id(cur))
        yield cur
        # Cause / context chain (the existing single-hop walk).
        for nxt in (cur.__cause__, cur.__context__):
            if nxt is not None:
                queue.append((nxt, depth + 1))
        # PEP 654 ExceptionGroup children — fan-out at parent depth.
        # Nested groups recurse via the queue.
        if isinstance(cur, BaseExceptionGroup):
            for child in cur.exceptions:
                queue.append((child, depth))


def _parse_url_int_or_none(key: str, raw: str, *, upper: int) -> int | None:
    """Strict int-or-``None`` parser for URL row/frame governors.

    Accepts:

    * ``"none"`` (case-insensitive) → ``None`` — disables the cap,
      mirroring the dbapi ``connect(max_total_rows=None)`` capability
      so URL-driven config (twelve-factor, env-var-driven engines)
      can express the same intent.
    * Any integer in ``1..upper`` — returns the int.

    Raises ``ArgumentError`` on anything else so a typo (``-1``,
    ``"infinite"``, garbage) fails at URL-parse time instead of much
    later when the caller tries to fetch.
    """
    token = raw.strip().lower()
    if token == "none":
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as e:
        raise ArgumentError(
            f"URL query {key}={raw!r} must be a positive integer or 'none' to disable: {e}"
        ) from e
    if not (0 < value <= upper):
        raise ArgumentError(
            f"URL query {key}={raw!r} is out of range (1..{upper}, or 'none' to disable)"
        )
    return value


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

    def bind_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            # Pysqlite's DateTime.bind_processor widens a bare
            # ``datetime.date`` to a midnight ``datetime`` before
            # handing it to the driver, so a ``date`` bound to a
            # ``DateTime`` column stores the canonical full-timestamp
            # shape (``"2021-03-15 00:00:00.000000"``) that a sibling
            # pysqlite writer would produce. Without this widening,
            # dqlitedbapi receives the raw ``date`` and encodes it as
            # a date-only ISO string (``"2021-03-15"``); round-trip
            # through the result_processor still works (ISO8601 is
            # bidirectional) but cross-writer parity breaks for
            # applications with literal-string predicates.
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                value = datetime.datetime.combine(value, datetime.time())
            return value

        return process

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
                    # DateTime(timezone=True) contract promises an aware
                    # value. A cell written without a tz suffix decodes
                    # as naive; attach UTC so downstream .astimezone /
                    # aware-vs-aware comparisons don't raise TypeError.
                    if value.tzinfo is None:
                        return value.replace(tzinfo=datetime.UTC)
                    return value
                # DateTime(timezone=False): the ORM field sees a naive
                # wall-clock interpreted as UTC. Convert through UTC
                # first so a non-UTC aware input (e.g. another writer's
                # local-offset datetime) has its actual instant
                # preserved — not just the wall-clock digits.
                if value.tzinfo is not None:
                    return value.astimezone(datetime.UTC).replace(tzinfo=None)
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


class _DqliteTime(sqltypes.Time):
    """Time processor handling ``datetime.time`` and ISO8601-string inputs.

    Pysqlite's ``TIME.result_processor`` calls ``processors.str_to_time``
    (a thin wrapper over ``datetime.time.fromisoformat``) on the raw
    cell. dqlitedbapi already decodes ISO8601 time payloads into
    ``datetime.time`` before the dialect sees them, so the upstream
    processor would call ``fromisoformat`` on a ``datetime.time``
    instance and raise ``TypeError``.

    Mirror ``_DqliteDateTime`` / ``_DqliteDate``: pass ``datetime.time``
    through unchanged, parse ``str`` (affinity-stripped cell from
    ``func.time(col)`` etc.) via ``datetime.time.fromisoformat``, log
    and pass through unparseable strings rather than crashing the read.

    No ``bind_processor`` override is needed: dqlitedbapi accepts
    ``datetime.time`` on the bind path and encodes it as an ISO8601
    string. A ``str`` bound to a ``Time`` column would be sent verbatim
    by the parent dialect's processor; we don't widen the contract
    here.
    """

    def bind_processor(self, dialect: Any) -> None:
        return None

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.time):
                return value
            if isinstance(value, str):
                try:
                    return datetime.time.fromisoformat(value)
                except ValueError as e:
                    logger.warning(
                        "Time processor received unparseable ISO8601 string %r: %s",
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

    # Enable SQLAlchemy statement caching.
    #
    # SA's ``_supports_statement_cache`` discovery reads
    # ``__class__.__dict__.get("supports_statement_cache")`` (see
    # ``engine/default.py::_supports_statement_cache``), not the
    # inherited attribute. Inheriting from the parent SQLite dialect —
    # even though it also sets True — would silently disable
    # statement caching here. Mirror the explicit pin and rationale
    # documented at ``aio.py``'s ``DqliteDialect_aio``.
    supports_statement_cache = True

    # dqlitedbapi returns native Python ``bytes`` for BLOB columns (the
    # wire codec emits ``bytes`` for ``ValueType.BLOB`` on the result
    # path). Pin True locally so ``LargeBinary.result_processor`` can
    # skip the redundant ``bytes(value)`` wrap on every BLOB cell.
    returns_native_bytes = True

    # dqlitedbapi cursors are buffered with continuation streaming
    # (frames fully consumed client-side); they do not implement
    # SQLAlchemy's server-side cursor protocol. The inherited
    # ``DefaultDialect.supports_server_side_cursors = False`` is
    # currently safe, but an explicit pin defends against any future
    # upstream change that flips the inherited default. Mirrors the
    # explicit pin on ``DqliteDialect_aio`` (aio.py).
    supports_server_side_cursors = False

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
        sqltypes.Time: _DqliteTime,
    }

    def __init__(self, **kwargs: Any) -> None:
        # Keyword-only: ``SQLiteDialect.__init__`` defines a handful of
        # keyword slots (``native_datetime``, ``json_serializer``, ...)
        # followed by ``**kw`` that flows into ``DefaultDialect``, which
        # in turn accepts a long list of positional slots
        # (``paramstyle``, ``isolation_level``, ``dbapi``, ...).
        # Forwarding ``*args`` would silently bind a caller's positional
        # value to whichever slot lines up in the parent's signature —
        # never what a third-party caller constructing the dialect
        # directly intended. SA-internal dialect construction always
        # passes kwargs, so narrowing the signature is non-breaking for
        # the engine-factory path and closes the positional-foot-gun.
        #
        # ``DefaultDialect.__init__`` accepts a ``paramstyle`` kwarg
        # and assigns it to ``self.paramstyle`` — overwriting the
        # class-level ``"qmark"`` pin. The dbapi only accepts qmark;
        # silently accepting ``paramstyle="named"`` would compile SQL
        # with ``:name`` placeholders that produce cryptic
        # ProgrammingError at execute time. Reject the kwarg up-front
        # so the user sees a config-time ArgumentError.
        if "paramstyle" in kwargs and kwargs["paramstyle"] != "qmark":
            raise ArgumentError(
                f"dqlite dialect requires paramstyle='qmark'; got {kwargs['paramstyle']!r}"
            )
        super().__init__(**kwargs)
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
        # Returns the top-level ``dqlitedbapi`` module exposing the sync
        # ``Connection`` / ``Cursor``. The async dialect overrides this
        # to return the ``dqlitedbapi.aio`` submodule (see
        # ``DqliteDialect_aio.import_dbapi``); the asymmetry is
        # deliberate.
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
    # silently disabling the client-layer ceiling. The bounds are
    # pragmatic: 2**31-1 rows and 10x the default frame cap leave
    # plenty of headroom for real workloads while refusing values
    # only a typo would supply.
    #
    # ``max_total_rows`` and ``max_continuation_frames`` accept the
    # literal token ``"none"`` (case-insensitive) → ``None`` to disable
    # the cap, mirroring the dbapi ``connect(max_total_rows=None)``
    # capability so URL-driven config (twelve-factor, env-var-driven
    # engines) can express the same intent. The 1_000_000 frame
    # ceiling is the dialect's own defense-in-depth cap; the dbapi /
    # wire layers do not enforce a hard ceiling.
    _URL_QUERY_ALLOWED: dict[str, tuple[Callable[[str], Any], Callable[[Any], bool] | None]] = {
        "timeout": (float, lambda v: math.isfinite(v) and v > 0),
        "max_total_rows": (
            lambda s: _parse_url_int_or_none("max_total_rows", s, upper=2**31 - 1),
            None,
        ),
        "max_continuation_frames": (
            lambda s: _parse_url_int_or_none("max_continuation_frames", s, upper=1_000_000),
            None,
        ),
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
        only SERIALIZABLE as a level we can actually honour.

        ``AUTOCOMMIT`` is also advertised — NOT as a level we accept,
        but so SA's engine flow (``_assert_and_set_isolation_level``)
        passes the value through to our ``set_isolation_level`` below
        rather than rejecting it with the generic
        "invalid isolation level" ``ArgumentError``. The dialect's
        dedicated rejection message (see ``set_isolation_level``)
        explains *why* autocommit is unsupported, which is strictly
        more actionable than the generic error. The advertised value
        is a diagnostic channel, not an acceptance claim.
        """
        return ["SERIALIZABLE", "AUTOCOMMIT"]

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

    def detect_autocommit_setting(self, dbapi_conn: DBAPIConnection) -> bool:
        """dqlite never operates in autocommit mode.

        Every statement traverses Raft consensus under an explicit
        transaction lifecycle (see ``set_isolation_level`` rejection of
        ``"AUTOCOMMIT"`` and the rationale at line 558-562). The dqlite
        dbapi ``Connection`` deliberately does NOT expose an
        ``isolation_level`` attribute; the inherited ``DefaultDialect``
        implementation raises ``NotImplementedError``, and the pysqlite
        sibling probes ``dbapi_conn.isolation_level is None`` which
        would also fail here. Both surface inside SA's
        ``skip_autocommit_rollback`` path
        (``engine/default.py::do_rollback`` ->
        ``engine/base.py:1115-1124``), where the user's SQL is healthy
        but the close path crashes with a confusing diagnostic.

        Returning False unconditionally makes ``skip_autocommit_rollback``
        a safe no-op for the dqlite dialect.
        """
        return False

    # do_rollback / do_commit are intentionally left inherited from the
    # parent dialect. The "cannot commit/rollback — no transaction is
    # active" error is swallowed at the DBAPI layer (dqlitedbapi's
    # Connection.commit / rollback), so the dialect doesn't need its own
    # workaround. Matches stdlib sqlite3 semantics.
    #
    # do_begin, by contrast, MUST be overridden. SA's parent
    # SQLiteDialect_pysqlite.do_begin is ``pass`` because pysqlite's
    # stdlib driver auto-emits BEGIN before the first DML via the
    # connection-level ``isolation_level`` attribute. The dqlite dbapi
    # has no such auto-BEGIN mechanism — without an explicit BEGIN over
    # the wire the server auto-commits each statement and engine.begin()
    # blocks would not be atomic (every INSERT independently committed,
    # ROLLBACK a no-op). Plain ``BEGIN`` (= BEGIN DEFERRED) matches
    # ``_TRANSACTION_BEGIN_SQL`` in dqliteclient and the C/Go peer
    # clients; dqlite serialises writes through Raft regardless of the
    # qualifier so IMMEDIATE / EXCLUSIVE would have no semantic effect.
    # Errors propagate unwrapped — SA's Connection._begin_impl wraps the
    # call in _handle_dbapi_exception, so is_disconnect classification
    # and pool-invalidation kick in for transport-level BEGIN failures.
    def do_begin(self, dbapi_connection: DBAPIConnection) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("BEGIN")
        finally:
            # Wrap close in a narrow defensive block so a transport-
            # class failure here (leader flip mid-BEGIN, dead socket
            # post-BEGIN) does not mask the BEGIN-time exception. The
            # close-time exception would replace the BEGIN one in
            # Python's finally semantics; the BEGIN exception then
            # only survives on ``__context__``, which SA's
            # ``is_disconnect`` cause-walk does NOT consult.
            try:
                cursor.close()
            except _TRANSPORT_CLASS_EXCEPTIONS:
                # Narrowed from the broader ``DatabaseError`` umbrella to
                # the shared transport-class tuple. An ``IntegrityError``
                # from ``cursor.close()`` is implausible (close doesn't
                # fire constraints) and would more likely indicate a
                # custom audit trigger or an outright programmer bug —
                # let it surface instead of silently masking the
                # BEGIN-time exception.
                logger.debug(
                    "do_begin: cursor.close failed after BEGIN; BEGIN exception preserved",
                    exc_info=True,
                )

    # Patterns are matched case-insensitively at the comparison site.
    # Stored in lower-case so the single ``.lower()`` at each
    # ``is_disconnect`` call normalises both sides; the previous
    # (``"not connected"`` plus ``"Not connected"``) duplicate is now
    # one entry.
    _dqlite_disconnect_messages = (
        "connection closed",
        "timed out",
        "failed to connect",
        "not connected",
        # Wire-layer desync: ProtocolError / DecodeError surface here
        # via the dbapi wrap at ``cursor._call_client`` that routes
        # ``client.ProtocolError`` to ``OperationalError(code=None)``.
        # The literal substring ``"wire decode failed"`` is the
        # canonical prefix emitted by ``dqliteclient/protocol.py``.
        "wire decode failed",
        # ``await_only`` raises ``RuntimeError("<Future ... attached
        # to a different loop>")`` when an ``AsyncConnection`` is
        # reused across event loops. The async adapter's
        # ``_handle_exception`` remaps that to ``OperationalError``
        # with the substring preserved so this branch can classify it.
        "different loop",
        # ``dqlitedbapi.AsyncConnection`` raises ``ProgrammingError``
        # with the wording ``"...different event loop"`` (note the
        # ``"event "`` between ``"different"`` and ``"loop"``, so this
        # is a DISTINCT substring from ``"different loop"`` above —
        # not a superstring). The async adapter's ``_handle_exception``
        # remaps that to ``OperationalError`` with the wording
        # preserved; without this entry the remapped error would not
        # match the substring scan and the cross-loop fault would
        # survive in the SA pool slot.
        "different event loop",
    )

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """Detect whether an exception indicates a broken connection.

        Prefer exception-type dispatch over message matching; the C
        server's error wording is not a contract. Type-based checks
        cover TCP resets, DNS failures, and partial-read timeouts that
        the hand-maintained substring list misses.

        Asymmetry with ``do_ping``: ``do_ping`` catches
        ``ProgrammingError`` and treats it as "slot is dead" because
        its only operation is a trivial ``SELECT 1``, where a
        ProgrammingError can only be an out-of-band state fault —
        specifically the cross-event-loop reuse shape that
        ``dqlitedbapi.AsyncConnection`` raises from
        ``_ensure_locks`` / ``cursor()``
        (see ``aio/connection.py:166-172, 418-433``). During a real
        query, a bare ProgrammingError is more likely a caller bug
        (passed wrong number of binds, used closed cursor in userland
        code) and must propagate so the bug is visible.

        Closed-handle surfaces as ``InterfaceError("Cursor is
        closed")`` / ``InterfaceError("Connection is closed")``, NOT
        ProgrammingError; the ``InterfaceError`` substring branch
        below classifies those as disconnect because they specifically
        indicate the SA pool slot itself is invalidated.

        For loop-mismatch ProgrammingError on a real-query path, the
        async adapter's ``_handle_exception`` remaps the exception to
        ``OperationalError("event-loop mismatch: ...")`` so the
        ``"different loop"`` substring branch picks it up. Without
        that remap the slot would survive a cross-loop fault and the
        next checkout would hit it again.
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
        # OS-level transport failures (socket RST, broken pipe, DNS,
        # connect refused, connection timeout). ``ConnectionError``,
        # ``BrokenPipeError``, and ``TimeoutError`` are all ``OSError``
        # subclasses, so the single ``OSError`` check covers every
        # stdlib transport-error shape (including ConnectionResetError
        # / ConnectionAbortedError / ConnectionRefusedError /
        # socket.gaierror that a narrower enumeration would miss).
        # Check on the bare exception before the walk: if ``e`` itself
        # is an OSError that's enough.
        if isinstance(e, OSError):
            return True

        # Single cause-chain walk applying every classification per
        # node. Order within the loop body matters: ClusterPolicyError
        # short-circuits before its parent ClusterError; the leader-
        # change code check runs before the substring scan so a coded
        # leader-flip OperationalError doesn't get gated out by the
        # ``code is None`` substring restriction; the substring scan
        # is the SOLE classifier for wire-decode / cross-loop
        # OperationalError(code=None), so it must come last.
        #
        # Replaces the earlier four-walks-per-call shape: each call
        # rebuilt the BFS queue and visited set, costing O(4N) where
        # the unified walk is O(N). For typical chains N is small; the
        # bigger win is consolidating the per-node ordering invariant
        # (a future predicate change updates one block, not four).
        # Legacy substring fallback — kept so we still catch anything
        # that wasn't modelled as a specific exception type yet. Match
        # case-insensitively: wire-layer / client-layer message
        # formatting is not a contract, and a future uppercase-leading
        # rewording would otherwise drop the match silently. Walk the
        # cause chain so a wrapped OperationalError (telemetry
        # middleware, retry decorators) does not silently bypass the
        # substring classifier — same discipline as the type/code
        # branches above. Wire-decode errors surface as
        # ``OperationalError(message, code=None)``, so the substring
        # branch is the SOLE classifier for those — any wrap would
        # otherwise defeat disconnect detection entirely.
        #
        # Widened to ``DatabaseError`` (parent of ``OperationalError``):
        # codes 11/24/26 (CORRUPT/FORMAT/NOTADB) now route to bare
        # ``DatabaseError`` per ``_classify_operational``, so a server
        # message with a wire-tail substring ("wire decode failed",
        # "wire stream error") on one of those codes would bypass the
        # ``OperationalError``-only check.
        #
        # Read ``raw_message`` first: the dbapi truncates the displayed
        # ``message`` argument at 1024 chars (``_MAX_DISPLAY_MESSAGE``)
        # at construction time for log hygiene, and preserves the full
        # server text on ``raw_message``. A disconnect substring past
        # byte 1024 would otherwise be invisible to ``str(cause)``
        # (which returns the truncated ``args[0]``).
        # Restrict the substring scan to (a) ``OperationalError`` (the
        # historical surface — wire-decode/transport failures) and (b)
        # bare ``DatabaseError`` with codes 11/24/26 (CORRUPT / FORMAT /
        # NOTADB) — the codes that motivated the round-2 widening to
        # ``DatabaseError``. Without the code-restriction on the
        # DatabaseError branch, a server-supplied user-defined error
        # message inside an ``IntegrityError`` (e.g. ``RAISE(ABORT,
        # '...timed out validating peer')``) would match the loose
        # ``"timed out"`` substring and be classified as a disconnect.
        # SA pool would then invalidate-and-retry — duplicating
        # non-idempotent INSERTs. The code set is hoisted to a module-
        # level frozen constant so a future addition / removal updates
        # one place (and so ``do_ping``'s parallel arm references the
        # same set).
        for cause in _walk_cause_chain(e):
            # Order: policy short-circuit FIRST (ClusterPolicyError
            # subclasses ClusterError, must short-circuit before the
            # broader transport-class check below).
            if isinstance(cause, _client_exc.ClusterPolicyError):
                return False
            # Transport-class direct hits.
            if isinstance(cause, (_client_exc.DqliteConnectionError, _client_exc.ClusterError)):
                return True
            # Closed-handle InterfaceError surface.
            if isinstance(cause, _dbapi_exc.InterfaceError):
                message = str(cause).lower()
                if "connection is closed" in message or "cursor is closed" in message:
                    return True
            # Leader-change code on either OperationalError shape —
            # checked before the substring scan so a coded leader-flip
            # is not gated out by the OE-arm code-is-None restriction
            # below.
            for err_class in (_dbapi_exc.OperationalError, _client_exc.OperationalError):
                if (
                    isinstance(cause, err_class)
                    and getattr(cause, "code", None) in _LEADER_CHANGE_CODES
                ):
                    return True
            # Substring scan — restricted to OperationalError(code=None)
            # (the wire-decode / ProtocolError / cross-loop-remap
            # surface) and bare DatabaseError with codes 11/24/26
            # (CORRUPT / FORMAT / NOTADB). Server-routed coded
            # OperationalErrors carry user-controlled message text and
            # must NOT trip disconnect classification on a benign
            # RAISE that happens to contain a transport-style
            # substring. Use ``raw_message`` first so a >1024-char
            # server message whose disconnect substring sits past the
            # truncation boundary is still classified.
            if isinstance(cause, _dbapi_exc.OperationalError):
                applies_substring = getattr(cause, "code", None) is None
            elif isinstance(cause, _dbapi_exc.DatabaseError):
                applies_substring = getattr(cause, "code", None) in _BARE_DBE_DISCONNECT_CODES
            else:
                applies_substring = False
            if applies_substring:
                text = getattr(cause, "raw_message", None) or str(cause)
                msg_lower = text.lower()
                for pattern in self._dqlite_disconnect_messages:
                    if pattern in msg_lower:
                        return True
        return super().is_disconnect(e, connection, cursor)

    # Two-phase commit is not supported by dqlite (no XA transaction
    # coordinator on the server). ``requirements.py`` already declares
    # ``two_phase_transactions = exclusions.closed()`` so the SA
    # compliance suite skips the corresponding tests; the runtime
    # overrides below surface the unavailability via PEP 249's
    # ``NotSupportedError`` rather than the inherited ``DefaultDialect``
    # ``NotImplementedError``. Callers writing
    # ``except sqlalchemy.exc.NotSupportedError`` to detect "feature
    # unavailable in this backend" then catch the dqlite case without
    # needing a duplicate ``except NotImplementedError`` branch only
    # for this dialect.
    def do_begin_twophase(self, connection: Any, xid: Any) -> None:
        raise _dbapi_exc.NotSupportedError(
            "dqlite does not support two-phase commit; use single-engine transactions instead."
        )

    def do_prepare_twophase(self, connection: Any, xid: Any) -> None:
        raise _dbapi_exc.NotSupportedError("dqlite does not support two-phase commit.")

    def do_commit_twophase(
        self,
        connection: Any,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
        raise _dbapi_exc.NotSupportedError("dqlite does not support two-phase commit.")

    def do_rollback_twophase(
        self,
        connection: Any,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
        raise _dbapi_exc.NotSupportedError("dqlite does not support two-phase commit.")

    def do_recover_twophase(self, connection: Any) -> list[Any]:
        raise _dbapi_exc.NotSupportedError("dqlite does not support two-phase commit.")

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Check if the connection is still alive.

        Only connection-level exceptions are interpreted as "dead"; any
        other exception propagates so the caller can see real bugs
        instead of having them silently rewritten as "please reconnect."

        ``DatabaseError`` is included for codes 11/24/26
        (CORRUPT/FORMAT/NOTADB) which ``_classify_operational`` routes
        to bare ``DatabaseError`` — pre-ping's purpose is "is this slot
        usable now?", and a node responding with CORRUPT to ``SELECT 1``
        is not, regardless of whether the underlying database is
        recoverable. Treat as ping-failure so the pool can try a
        different node.
        """
        cursor = dbapi_connection.cursor()
        try:
            try:
                cursor.execute("SELECT 1")
                return True
            except (
                # Narrow per-class catch instead of the ``DatabaseError``
                # umbrella so ``IntegrityError`` / ``DataError`` /
                # ``InternalError`` / ``NotSupportedError`` from a buggy
                # SELECT-trigger setup propagate as real errors instead
                # of being silently rewritten as "ping failed; reconnect
                # the slot." The classes below cover the practical pre-
                # ping faults:
                #   * ``OperationalError`` — historical case
                #     (transient/permanent server-reported faults).
                #   * ``ProgrammingError`` — cross-loop reuse from
                #     ``AsyncConnection._ensure_locks`` / ``cursor()``;
                #     a permanent per-slot fault.
                #   * ``InterfaceError`` — closed cursor / connection.
                _dbapi_exc.OperationalError,
                _dbapi_exc.ProgrammingError,
                _dbapi_exc.InterfaceError,
                _client_exc.DqliteConnectionError,
                OSError,
            ):
                return False
            except _dbapi_exc.DatabaseError as exc:
                # Bare ``DatabaseError`` for codes 11/24/26
                # (CORRUPT/FORMAT/NOTADB) — pre-ping reports the slot
                # as unusable so the pool invalidates it; a follow-up
                # checkout may land on a healthy node. Other coded
                # ``DatabaseError`` subclasses (Integrity / Data /
                # Internal / NotSupported) propagate so a buggy setup
                # surfaces.
                if getattr(exc, "code", None) in _BARE_DBE_DISCONNECT_CODES:
                    return False
                raise
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
                # See the outer ``except`` rationale — same umbrella so
                # ``cursor.close()`` failures from CORRUPT/FORMAT/NOTADB
                # are also debug-logged rather than crashing the ping.
                _dbapi_exc.DatabaseError,
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
