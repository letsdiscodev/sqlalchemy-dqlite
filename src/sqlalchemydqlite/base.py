"""Base dqlite dialect for SQLAlchemy."""

import datetime
import logging
import math
import types
from collections.abc import Callable, Iterator, Sequence
from typing import Any, ClassVar, Final

from sqlalchemy import pool, util
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite.base import SQLiteCompiler
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from dqlitewire import LEADER_ERROR_CODES, SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB
from dqlitewire.constants import DQLITE_PROTO
from dqlitewire.messages.responses import _sanitize_server_text

# InterfaceError codes that originate server-side and may carry a
# transport-style message that the substring scanner should classify
# as a disconnect. ``DQLITE_PROTO`` (1001) covers protocol-misuse
# replies the C server emits via ``gateway.c::handle_request_*``; a
# "wire decode failed" wording embedded in such a reply must
# invalidate the SA pool slot.
#
# SQLITE_RANGE (25) and SQLITE_MISUSE (21) are explicitly EXCLUDED:
# they are caller-side bind/use bugs (``cursor.execute("SELECT ?",
# ())``, ``library used incorrectly``) — surfacing them as
# disconnects would silently retry a permanent caller bug against
# a fresh connection. An earlier broadening of
# ``applies_substring = code is not None`` over-included them; this
# constant restores the narrow contract.
_SERVER_INTERFACEERROR_DISCONNECT_CODES: Final[frozenset[int]] = frozenset({DQLITE_PROTO})

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
_BARE_DBE_DISCONNECT_CODES: Final[frozenset[int]] = frozenset(
    {SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB}
)
# **Forward-compat note**: dqlitedbapi's ``_CODE_TO_EXCEPTION`` also
# routes ``SQLITE_NOLFS`` (22), ``SQLITE_AUTH`` (23), ``SQLITE_NOTICE``
# (27), and ``SQLITE_WARNING`` (28) to bare ``DatabaseError``. dqlite-
# server doesn't currently emit any of those four codes on the wire,
# so they are intentionally NOT included here — they are deterministic
# non-transient diagnostics, not slot-fatal conditions. If a future
# server release starts emitting them with a transport-class message,
# the substring scan in ``is_disconnect`` will still classify them
# correctly because the message wording (``"wire decode failed"``,
# ``"event loop closed"``, etc.) is the load-bearing discriminator on
# bare DatabaseError, NOT the code. The set remains the
# ``CORRUPT``/``FORMAT``/``NOTADB`` triad for which "kill the slot"
# is unambiguous regardless of the message.

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
_TRANSPORT_CLASS_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    _dbapi_exc.OperationalError,
    _dbapi_exc.InterfaceError,
    _client_exc.DqliteConnectionError,
    OSError,
)

logger = logging.getLogger(__name__)


# Cap server-controlled TEXT cells before they enter %r-formatted log
# lines on the result_processor parse-failure path. The wire layer caps
# each TEXT cell at _MAX_TEXT_VALUE_SIZE = 16 MiB; %r roughly doubles
# the size with quoting/escapes, multiplied across each row of a result
# set. Without truncation, a malicious or compromised server returning
# oversized TEXT for a column the dialect maps to DateTime / Date /
# Time produces gigabytes of log output per query — DoS class against
# operator log infrastructure (rsyslog, journald rate limiters, log
# shippers). 200 chars matches the cap discipline used elsewhere in the
# project (cluster._truncate_error, dbapi.exceptions truncation).
_LOG_TRUNCATE_MAX_CHARS: Final[int] = 200


def _truncate_for_log(value: str) -> str:
    """Return ``value`` truncated to ``_LOG_TRUNCATE_MAX_CHARS`` chars
    with a marker noting how many chars were dropped, suitable for
    embedding into a ``logger.warning(..., %r, ...)`` format. Pure;
    no-op when ``value`` is already within the cap.

    Truncation only — does NOT sanitize control / bidi / invisible
    characters. Use :func:`_safe_for_log` at sites where the input
    is server-controlled (row payloads, error messages echoed back).
    """
    if len(value) <= _LOG_TRUNCATE_MAX_CHARS:
        return value
    overflow = len(value) - _LOG_TRUNCATE_MAX_CHARS
    return f"{value[:_LOG_TRUNCATE_MAX_CHARS]}... [truncated, {overflow} chars]"


def _safe_for_log(value: str) -> str:
    """Sanitize control / bidi / invisible characters AND truncate
    for log embedding. Use at any site interpolating a server-
    controlled string (row TEXT cells, server-emitted error
    messages) into a log line.

    ``%r`` (Python repr) escapes ``\\n`` / ``\\r`` / ``\\t`` but does
    NOT escape U+2028 / U+2029 / RTL-override / ZWSP — journald
    treats U+2028 as a record separator, so a row payload
    containing it lands in the log as a separate record (log-
    injection vector). Composing with the wire-side
    ``_sanitize_server_text`` (which strips C0/C1, U+2028/U+2029,
    full bidi block, ZW chars, BOM) closes that gap before
    truncation.
    """
    return _truncate_for_log(_sanitize_server_text(value))


__all__ = ["DqliteCompiler", "DqliteDialect"]

_TRUE_TOKENS: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS: Final[frozenset[str]] = frozenset({"0", "false", "no", "off"})

# Single source of truth for the AUTOCOMMIT-rejection diagnostic. Used
# by the eager dialect-init reject (positional foot-gun guard at
# ``__init__``), by ``set_isolation_level`` (SA's connect-listener
# step), and by ``AsyncAdaptedConnection.autocommit.setter`` (the
# bottom-layer dbapi setter). Keeping the wording in one place
# guarantees the four reject-sites stay in lockstep — earlier the
# message lived in three independent string literals and a future
# wording change would have drifted across them.
_AUTOCOMMIT_REJECTION_MSG: Final[str] = (
    "dqlite does not support SA's AUTOCOMMIT isolation level; "
    "the SA dialect always brackets statements in BEGIN / COMMIT "
    "for transactional control. Use explicit commit() / "
    "rollback() on the connection. (The underlying wire is "
    "autocommit-by-default; this is about SA's transaction "
    "model, not the wire.)"
)


def _walk_cause_chain(e: BaseException, max_depth: int = 25) -> Iterator[BaseException]:
    """Yield ``e`` and each ``__cause__`` / ``__context__`` /
    ``BaseExceptionGroup.exceptions`` child up to ``max_depth``.

    The ``visited`` set prevents an infinite loop on pathological
    cycles (``raise X from X`` or a deeply-nested wrap that loops
    back). The depth cap is a second line of defence so a truly
    degenerate chain cannot drag classifier latency even if the
    visited-set catch misses for some reason. Same shape as
    ``traceback.TracebackException.__init__``'s ``_seen``-set
    cause/context traversal, extended with PEP 654
    ``ExceptionGroup`` children.

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
            # Cross-type rejection: bind-side mirror of the
            # result-side raise. A ``datetime.time`` bound to a
            # DateTime column would be encoded by dqlitedbapi as a
            # time-only ISO string (``"HH:MM:SS"``); on readback the
            # result_processor branch raises ``DataError``
            # ("no defensible date to fabricate"). Without this
            # bind-side rejection, the same dialect's bind+read
            # round-trip writes a cell that the same dialect's reader
            # rejects. Pysqlite raises TypeError on the same input.
            # Ordered before the date-widen branch because
            # ``datetime.time`` is not a ``datetime.date`` subclass —
            # both branches need explicit handling.
            if isinstance(value, datetime.time) and not isinstance(value, datetime.datetime):
                raise _dbapi_exc.DataError(
                    f"DateTime column cannot bind time-only payload "
                    f"{value!r}: no defensible date to fabricate."
                )
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
        # One-shot WARNING gate per processor instance. A SELECT
        # against a corrupted column would otherwise emit one WARNING
        # per row (up to ``max_total_rows``), drowning operator log
        # pipelines. Mirrors the ``_max_total_rows_disabled_warning_emitted``
        # one-shot pattern at base.py:1224. Subsequent bad rows
        # in the same processor instance demote to DEBUG.
        warned: list[bool] = [False]

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, str):
                # Affinity-stripped cell: TEXT-tagged on the wire, so
                # dqlitedbapi did not run its datetime converter.
                try:
                    value = datetime.datetime.fromisoformat(value)
                except ValueError as e:
                    if not warned[0]:
                        warned[0] = True
                        logger.warning(
                            "DateTime processor received unparseable ISO8601 string %r: %s "
                            "(further unparseable rows in this processor demoted to DEBUG)",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
                        )
                    else:
                        logger.debug(
                            "DateTime processor received unparseable ISO8601 string %r: %s",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
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
            if isinstance(value, datetime.time):
                # Cross-type confusion: ``dqlitedbapi._datetime_from_iso8601``
                # is intentionally polymorphic and decodes a time-only
                # ISO string (e.g. ``"12:30:00"``) into ``datetime.time``.
                # If such a payload lands in a ``DateTime`` column,
                # silently passing it through would feed the ORM
                # ``Row.x: datetime.datetime`` consumer a ``datetime.time``
                # — surfacing only as a far-from-cause attribute error
                # (``.year`` / ``.date()`` / etc.). Unlike the symmetric
                # ``Time``-receives-``datetime`` case (narrowed via
                # ``.time()``), there is no defensible coercion here:
                # fabricating a sentinel date would silently produce
                # bogus instant values. Raise so the type-contract
                # violation surfaces immediately.
                raise _dbapi_exc.DataError(
                    f"DateTime column received time-only payload "
                    f"{value!r}: the cell decodes as datetime.time and "
                    f"there is no defensible date to fabricate."
                )
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

    def bind_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            # Cross-type rejection: bind-side mirror of the
            # result-side raise. A ``datetime.time`` bound to a Date
            # column would encode as ``"HH:MM:SS"``; the result-side
            # raises ``DataError`` on that shape. Reject at bind so
            # the round-trip-self-rejection fork doesn't write a
            # cell the same dialect's reader rejects.
            if isinstance(value, datetime.time) and not isinstance(value, datetime.datetime):
                raise _dbapi_exc.DataError(
                    f"Date column cannot bind time-only payload "
                    f"{value!r}: a time has no date component."
                )
            # Narrow ``datetime.datetime`` to ``datetime.date`` for
            # pysqlite-parity wire format. Without this narrowing,
            # ``_iso8601_from_datetime`` writes a full
            # ``"YYYY-MM-DD HH:MM:SS"`` to a Date cell — breaking
            # cross-writer parity with pysqlite, which emits only
            # ``"YYYY-MM-DD"``. Sibling parity with
            # ``_DqliteDateTime.bind_processor``'s symmetric widen
            # in the reverse direction. tzinfo is dropped (matches
            # the result-side documented behaviour and pysqlite).
            if isinstance(value, datetime.datetime):
                return value.date()
            return value

        return process

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        # One-shot WARNING gate; see _DqliteDateTime.result_processor.
        warned: list[bool] = [False]

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                # Deliberate: tzinfo is dropped. See class docstring.
                return value.date()
            if isinstance(value, datetime.time):
                # Cross-type confusion mirror of ``_DqliteDateTime``:
                # ``dqlitedbapi._datetime_from_iso8601`` decodes a
                # time-only ISO string into ``datetime.time``. If it
                # lands in a ``Date`` column, there is no defensible
                # date to fabricate (analogous to the
                # ``DateTime``-receives-``time`` case). Raise rather
                # than silently leak the wrong concrete type.
                raise _dbapi_exc.DataError(
                    f"Date column received time-only payload "
                    f"{value!r}: the cell decodes as datetime.time and "
                    f"there is no defensible date to fabricate."
                )
            if isinstance(value, str):
                try:
                    return datetime.date.fromisoformat(value)
                except ValueError as e:
                    if not warned[0]:
                        warned[0] = True
                        logger.warning(
                            "Date processor received unparseable ISO8601 string %r: %s "
                            "(further unparseable rows in this processor demoted to DEBUG)",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
                        )
                    else:
                        logger.debug(
                            "Date processor received unparseable ISO8601 string %r: %s",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
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
        want_timezone = self.timezone
        # One-shot WARNING gate; see _DqliteDateTime.result_processor.
        warned: list[bool] = [False]

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                # Cross-type confusion: ``dqlitedbapi._datetime_from_iso8601``
                # is intentionally polymorphic and decodes a full
                # ``"YYYY-MM-DD HH:MM:SS"`` ISO string into
                # ``datetime.datetime``. If such a payload lands in a
                # ``Time`` column, narrow via ``.time()`` — sibling
                # parity with ``_DqliteDate``'s ``datetime -> date``
                # narrowing at line ~284. The date component is
                # silently dropped (mirroring ``_DqliteDate``'s
                # documented "tzinfo is dropped" decision); ``Time``
                # has no date dimension to preserve. ``isinstance``
                # check ordered before ``datetime.time`` because
                # ``datetime.datetime`` is **not** a ``datetime.time``
                # subclass — both branches need explicit handling.
                #
                # ``.time()`` always drops tzinfo (its tz-preserving
                # analogue is ``.timetz()``); the post-narrow value
                # falls through to the ``datetime.time`` branch below
                # so ``Time(timezone=True)`` re-attaches UTC and
                # ``Time(timezone=False)`` keeps it naive — symmetric
                # with how ``_DqliteDateTime`` handles its sibling
                # ``datetime`` payloads.
                value = value.time()
            if isinstance(value, datetime.time):
                if want_timezone:
                    # Time(timezone=True) contract promises an aware
                    # value. A cell written without a tz suffix
                    # decodes as naive; attach UTC so downstream
                    # aware-vs-aware comparisons don't raise
                    # TypeError. Symmetric with
                    # ``_DqliteDateTime`` at the equivalent branch.
                    if value.tzinfo is None:
                        return value.replace(tzinfo=datetime.UTC)
                    return value
                # Time(timezone=False) contract promises a naive
                # value. Strip tzinfo so the column type's contract
                # is honoured. ``datetime.time`` has no
                # ``astimezone`` analogue (it would need a date for
                # DST), so a fixed-offset conversion is unsafe in
                # general; pysqlite drops tzinfo unconditionally
                # too. Match the simpler "strip" semantics.
                if value.tzinfo is not None:
                    return value.replace(tzinfo=None)
                return value
            if isinstance(value, str):
                try:
                    return datetime.time.fromisoformat(value)
                except ValueError as e:
                    if not warned[0]:
                        warned[0] = True
                        logger.warning(
                            "Time processor received unparseable ISO8601 string %r: %s "
                            "(further unparseable rows in this processor demoted to DEBUG)",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
                        )
                    else:
                        logger.debug(
                            "Time processor received unparseable ISO8601 string %r: %s",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
                        )
                    return value
            return value

        return process


class DqliteCompiler(SQLiteCompiler):
    """SQLite-flavoured compiler with dqlite-specific overrides.

    dqlite has no UDF primitive — the wire protocol exposes prepared
    statements only, with no equivalent of pysqlite's
    ``connection.create_function``. Pysqlite's inherited
    ``visit_regexp_match_op_binary`` (and its negated sibling) emit
    ``col REGEXP ?`` / ``col NOT REGEXP ?`` SQL that depends on a
    runtime ``regexp`` UDF registered in pysqlite's ``on_connect``;
    our ``DqliteDialect.on_connect`` is a no-op (the dqlitedbapi
    ``Connection.create_function`` raises ``NotSupportedError``) so
    the inherited compile-then-execute path produces a late
    ``OperationalError: no such function: regexp`` from the cluster
    — by which time a SAVEPOINT may already be open and the user has
    paid the round-trip cost. Raise at compile time instead so the
    diagnostic surfaces at statement-construction.

    Both visitors are required: SA's ``SQLiteCompiler`` defines
    ``visit_not_regexp_match_op_binary`` as a separate dispatcher
    (``.venv/.../sqlite/base.py:1581``), the negation is NOT
    auto-derived from the positive form.

    The raise uses ``dqlitedbapi.exceptions.NotSupportedError`` rather
    than ``sqlalchemy.exc.CompileError`` for consistency with the
    dialect's other "this dbapi does not support X" raises (e.g., the
    two-phase commit pattern in ``DqliteDialect.do_begin_twophase``).
    The exception is a PEP 249 standard class and matches the dbapi
    surface a user already sees for similar capability gaps.
    """

    _REGEXP_MATCH_NOT_SUPPORTED_MSG = (
        "regexp_match is not available on dqlite: the wire protocol "
        "has no UDF primitive, so SQLite's REGEXP operator (which "
        "pysqlite implements via Connection.create_function) cannot "
        "be honoured. Filter with LIKE, or pre-compile the regex "
        "client-side."
    )

    def visit_regexp_match_op_binary(self, binary: Any, operator: Any, **kw: Any) -> str:
        raise _dbapi_exc.NotSupportedError(self._REGEXP_MATCH_NOT_SUPPORTED_MSG)

    def visit_not_regexp_match_op_binary(self, binary: Any, operator: Any, **kw: Any) -> str:
        raise _dbapi_exc.NotSupportedError(self._REGEXP_MATCH_NOT_SUPPORTED_MSG)


class DqliteDialect(SQLiteDialect_pysqlite):
    """SQLAlchemy dialect for dqlite.

    Inherits from ``SQLiteDialect_pysqlite`` — the canonical SA SQLite
    dialect parent that aiosqlite and pysqlcipher also build on. This
    surface picks up pysqlite's defaults (``default_paramstyle="qmark"``,
    ``returns_native_bytes=True``, ``description_encoding=None``,
    pysqlite-flavoured ``colspecs``) for free; the dqlite-specific
    deltas remain as explicit overrides below.

    Notable deliberate divergences from the parent (each documented at
    the override site):

    * ``import_dbapi`` returns ``dqlitedbapi`` instead of stdlib
      ``sqlite3``.
    * ``create_connect_args`` parses our ``dqlite://host:port/db?...``
      URL form rather than pysqlite's file-path / ``?uri=`` form.
    * ``set_isolation_level`` rejects ``AUTOCOMMIT`` (every dqlite
      statement goes through Raft consensus; per-statement autocommit
      is not a meaningful surface).
    * ``detect_autocommit_setting`` always returns False (same
      reason).
    * ``on_connect`` is a no-op — pysqlite registers ``regexp`` and
      ``floor`` UDFs via ``Connection.create_function``, which
      dqlitedbapi raises ``NotSupportedError`` on (the dqlite server
      has no user-defined-function primitive).
    * ``is_disconnect`` is a broad classifier walking exception
      cause chains; pysqlite's narrow substring check covers only
      the in-process sqlite3 case.
    """

    name = "dqlite"

    # One-shot warning gate for ``?max_total_rows=none`` (or
    # equivalent ``=disabled``) in the URL: emit the row-cap-disabled
    # WARNING once per dialect class, not once per connect / once per
    # engine. ``create_connect_args`` flips this to True after the
    # first emit.
    _max_total_rows_disabled_warning_emitted: ClassVar[bool] = False

    # Pin the default isolation level at class level so the contract is
    # evident statically and so test harnesses that build a dialect
    # without an engine (skipping ``DefaultDialect.initialize()``) still
    # see the right value. ``get_isolation_level`` always returns
    # ``"SERIALIZABLE"`` (dqlite is single-leader Raft so every
    # transaction is serialised at the cluster level), so the
    # ``initialize()`` path would set this slot to the same value
    # — the explicit pin is defensive drift defence.
    default_isolation_level = "SERIALIZABLE"

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        # dqlite is a remote dbapi: every connection is an independent
        # socket and must NOT share threads. ``QueuePool`` is the
        # ``DefaultDialect`` default — pin explicitly so a future
        # change to the SA default cannot silently flip dqlite to a
        # pool class with different concurrency semantics
        # (``SingletonThreadPool`` would multiplex one socket across
        # threads, breaking the dbapi's threadsafety=1 contract). The
        # async sibling at ``aio.py`` follows the same explicit-pin
        # discipline with ``AsyncAdaptedQueuePool``.
        return pool.QueuePool

    # ``driver`` is the dbapi-module-name convention (matches SA's
    # pysqlite reference: ``driver = "pysqlite"`` for the
    # ``sqlite3``-aliased-as-``pysqlite`` module). The async
    # sibling at ``aio.py`` uses ``driver = "aio"`` for URL-shape
    # parity with the user-typed ``dqlite+aio://...`` form. The
    # asymmetry is intentional: the sync URL ``dqlite://...`` has
    # NO ``+driver`` suffix, so the description-vs-URL parity
    # argument that motivated the async rename does not apply
    # here — there is no string the user "typed" in the URL to
    # mirror. Falling back to the dbapi-module-name convention
    # (``"dqlitedbapi"``) keeps the rendered ``dialect_description``
    # = ``"dqlite+dqlitedbapi"`` self-describing — the second half
    # is exactly what a user would import.
    #
    # SA's own dialect ecosystem is internally inconsistent on
    # this naming pattern (pysqlite = dbapi-module-name; aiosqlite
    # = both; aiomysql = dbapi-module-name; mariadbconnector =
    # dbapi-module-name). There is no single SA convention to
    # follow. See aio.py's ``driver = "aio"`` rationale block for
    # the async-side parity argument.
    driver = "dqlitedbapi"

    # ``paramstyle`` (qmark) inherited transitively: ``dqlitedbapi.paramstyle``
    # is "qmark", which SA's ``DefaultDialect.__init__`` reads via
    # ``self.dbapi.paramstyle`` and assigns to the dialect instance
    # before falling back to ``default_paramstyle``.
    # ``SQLiteDialect_pysqlite.default_paramstyle = "qmark"`` is the
    # secondary defence. No class-level override needed.

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

    # ``returns_native_bytes = True`` inherited from
    # ``SQLiteDialect_pysqlite`` (set explicitly in pysqlite's class
    # body). The original local pin (added when the parent was the
    # abstract ``SQLiteDialect``) cited a future ``DefaultDialect``
    # default flip as the drift surface — that surface no longer
    # exists: pysqlite's explicit True sits between us and
    # ``DefaultDialect``, and SA would not flip pysqlite's value to
    # False without a major performance regression for every pysqlite
    # user.
    #
    # dqlitedbapi returns native Python ``bytes`` for BLOB columns
    # (the wire codec emits ``bytes`` for ``ValueType.BLOB`` on the
    # result path), matching pysqlite's contract;
    # ``LargeBinary.result_processor`` skips the redundant
    # ``bytes(value)`` wrap on every BLOB cell.

    # Drift defence: pin ``description_encoding = None`` locally so the
    # contract is evident on the class without an MRO walk. dqlitedbapi
    # returns ``str`` column names (matches pysqlite); a non-``None``
    # value here would route descriptions through SA's byte-decode
    # pipeline (``cursor.description[i][0].decode(encoding)``), crashing
    # with ``AttributeError: 'str' object has no attribute 'decode'``.
    # Pysqlite already pins this (pysqlite.py:480); the local re-pin is
    # documentary parity with the rest of this drift-defence block —
    # purely a no-op today, future-proofs against an upstream refactor.
    description_encoding = None

    # dqlitedbapi cursors are buffered with continuation streaming
    # (frames fully consumed client-side); they do not implement
    # SQLAlchemy's server-side cursor protocol. The inherited
    # ``DefaultDialect.supports_server_side_cursors = False`` is
    # currently safe, but an explicit pin defends against any future
    # upstream change that flips the inherited default. Mirrors the
    # explicit pin on ``DqliteDialect_aio`` (aio.py).
    supports_server_side_cursors = False

    # SA's pool gates its forced-disposal path on ``has_terminate``
    # (see ``pool/base.py`` docs for ``_ConnectionRecord.invalidate``
    # and ``DefaultDialect.do_terminate`` line 717). The inherited
    # default is ``False`` (do_terminate falls back to do_close),
    # which would route engine.dispose's forced reclaim through
    # ``Connection.close()`` — bounded by ``self._timeout`` (default
    # 10 s, gated on a parked wire read). Under partition + SIGTERM
    # that 10 s blocks operator shutdown SLAs.
    #
    # Pin True locally so SA's terminate path lands on
    # :meth:`do_terminate` below, which calls the dbapi's bounded
    # ``force_close_transport`` (gated on ``close_timeout``, default
    # 0.5 s). The async sibling at ``aio.py`` carries the same pin.
    has_terminate = True

    # dqlite's wire protocol has a first-class BOOLEAN tag
    # (``ValueType.BOOLEAN = 11``); the server returns native Python
    # booleans for columns tagged BOOLEAN and dqlitedbapi passes them
    # through unchanged. Unlike the inherited pysqlite dialect
    # (``supports_native_boolean = False``), we don't need SQLAlchemy
    # to emit a ``CHECK (col IN (0, 1))`` constraint — the wire
    # contract enforces the 0/1 invariant.
    #
    # **Round-trip identity asymmetry — documented contract**:
    # The server sets the wire BOOLEAN tag based on the *declared
    # column type*, NOT the bind-time inference. A row inserted into a
    # column declared INTEGER (not BOOLEAN) — even when the bind value
    # was Python ``bool`` — comes back as an int (1 / 0) on readback,
    # NOT ``bool``. Application code that does ``if row.flag is True:``
    # against a BOOLEAN-typed column on the bind side and an INTEGER
    # column on the storage side will silently fail because the
    # readback is ``1``, not ``True`` (``==`` works; ``is True`` does
    # not). This asymmetry mirrors stdlib ``sqlite3`` behaviour for
    # SQLite-native INTEGER columns and is preserved here rather than
    # masked by a result_processor that coerces ``int → bool`` on
    # ``Boolean``-typed SA columns: the SA side declares the column
    # type and the user owns the contract.
    #
    # If round-trip ``bool`` identity matters, declare the SA column
    # ``Column(Boolean)`` (which materialises as a BOOLEAN-typed wire
    # column) and avoid raw INTEGER columns for boolean values.
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
    # ``update_returning_multifrom = True`` inherited from
    # ``SQLiteDialect`` (set in its class body, not its version gate).
    # The original local pin cited the RETURNING re-pin pattern for
    # drift defence, but the parent's version gate at
    # ``SQLiteDialect.__init__`` only resets the trio above —
    # multifrom is not in it. With the pysqlite parent, the inherited
    # True is stable.

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
    # executemany rowcounts. ``supports_sane_rowcount_returning`` is
    # False because SQLAlchemy's insertmanyvalues-with-RETURNING path
    # relies on separate accounting; pinning False matches the
    # inherited SQLiteDialect behaviour but, like the other pins
    # above, guards against silent upstream drift. There are three
    # rowcount-truthfulness flags total in SA 2.x — a phantom
    # ``supports_sane_multi_rowcount_returning`` line was previously
    # pinned here; SA does not define such an attribute, so the pin
    # was a no-op decorative entry that would have applied silently
    # to a future flag of that name with arbitrary semantics. Removed
    # to keep the drift-defence surface aligned with reality.
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_sane_rowcount_returning = False

    # Insert-path flags. SQLAlchemy's insertmanyvalues codegen,
    # DEFAULT VALUES form, and rowid handling all key on these.
    #
    # ``use_insertmanyvalues`` and ``insert_null_pk_still_autoincrements``
    # are pinned locally for drift defence: ``DefaultDialect`` defaults
    # could theoretically flip in a future SA release.
    # ``supports_default_values`` is also kept here even though
    # ``SQLiteDialect.__init__`` resets it via a version gate
    # (``self.dbapi.sqlite_version_info >= (3, 3, 8)``); the class-attr
    # pin sets the pre-``__init__`` baseline correctly so a hypothetical
    # bare-instantiated dialect (no ``__init__`` run) still reports True.
    use_insertmanyvalues = True
    supports_default_values = True
    insert_null_pk_still_autoincrements = True
    # NOTE: ``supports_default_metavalue = True``,
    # ``default_metavalue_token = "NULL"``, ``tuple_in_values = True``,
    # ``update_returning_multifrom = True``, ``supports_alter = False``,
    # and ``supports_empty_insert = False`` are inherited verbatim from
    # the parent (``SQLiteDialect_pysqlite`` → ``SQLiteDialect``, which
    # sets each in its class body). The original local pins were added
    # under the abstract ``SQLiteDialect`` parent and cited drift
    # surfaces (``DefaultDialect`` flip, hypothetical "version-gated
    # regression") that no longer apply: the parent's class-body value
    # is now the source of truth. Removed here; the matching tests in
    # ``tests/test_dialect.py`` were updated to assert the inherited
    # value without ``__class__.__dict__`` membership.

    # Override pysqlite's date/time processors with dqlite-specific
    # ones. ``SQLiteDialect_pysqlite.colspecs`` maps ``Date`` and
    # ``TIMESTAMP`` to ``_SQLite_pysqliteDate`` / ``_SQLite_pysqliteTimeStamp``,
    # which expect stdlib sqlite3 to have already decoded TEXT cells
    # via ``detect_types=PARSE_DECLTYPES``. dqlitedbapi has no such
    # auto-decode hook on the wire — TEXT cells reach the result-
    # processor as plain strings. Our processors handle the wire
    # shape directly (``str.fromisoformat`` for the string path,
    # passthrough for the ``datetime`` path). ``Time`` uses our
    # processor too because pysqlite has no ``Time`` colspec at all.
    # ``TIMESTAMP`` is mapped explicitly even though it would
    # transitively reach our ``DateTime`` processor via MRO — pinning
    # the entry guards against a future SA change to ``sqltypes.TIMESTAMP``'s
    # MRO that would otherwise re-bind to pysqlite's
    # ``_SQLite_pysqliteTimeStamp`` and raise ``TypeError`` on every
    # already-decoded ``datetime`` cell.
    colspecs = {
        **SQLiteDialect_pysqlite.colspecs,
        sqltypes.DateTime: _DqliteDateTime,
        sqltypes.Date: _DqliteDate,
        sqltypes.Time: _DqliteTime,
        sqltypes.TIMESTAMP: _DqliteDateTime,
    }

    # Override the inherited statement compiler with a subclass that
    # raises at compile time on ``regexp_match`` / its negation, since
    # dqlite has no UDF primitive to support SQLite's ``REGEXP``
    # operator. See ``DqliteCompiler`` for the full rationale.
    statement_compiler = DqliteCompiler

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
        # Symmetric eager rejection of ``isolation_level="AUTOCOMMIT"``.
        # ``set_isolation_level`` rejects the value at SA's connect-
        # listener step (``engine/default.py::_builtin_onconnect``),
        # but a deferred reject means ``create_engine()`` succeeds and
        # the error surfaces at first connect — far from the
        # configuration site, with a confusing pool-side traceback,
        # and on some pool configurations as a retry-masked transient
        # failure. Reject here so the diagnostic points at the kwarg.
        # Mirrors the paramstyle block above and the bottom-layer
        # ``AsyncAdaptedConnection.autocommit.setter`` guard.
        # SA's ``_assert_and_set_isolation_level`` does
        # ``.replace("_", " ").upper()`` then matches against
        # ``["SERIALIZABLE", "AUTOCOMMIT"]`` — so an underscore form
        # like ``"AUTO_COMMIT"`` becomes ``"AUTO COMMIT"`` and is
        # rejected by SA itself before reaching us. Only the
        # spaceless ``"AUTOCOMMIT"`` (case-insensitive) form would
        # otherwise reach the connect-listener step. Match that
        # form here so we eagerly reject what SA would have routed
        # to ``set_isolation_level``.
        iso_level = kwargs.get("isolation_level")
        if isinstance(iso_level, str) and iso_level.upper() == "AUTOCOMMIT":
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        # Symmetric eager rejection of ``native_datetime``. Pysqlite's
        # parent ``SQLiteDialect`` accepts the kwarg and gates pysqlite's
        # bespoke ``_SQLite_pysqliteTimeStamp`` / ``_SQLite_pysqliteDate``
        # processors on it. Our colspec-based date/time path doesn't go
        # through those processors — ``_DqliteDateTime`` / ``_DqliteDate``
        # subclass ``sqltypes.DateTime`` / ``sqltypes.Date`` directly —
        # so silently accepting ``native_datetime=True`` would be a
        # contract divergence vs the documented pysqlite semantics.
        # Reject up-front so the user sees a config-time
        # ``ArgumentError`` pointing at the kwarg, mirroring the
        # paramstyle / AUTOCOMMIT pattern above.
        if "native_datetime" in kwargs:
            raise ArgumentError(
                "dqlite dialect does not honour ``native_datetime``: the "
                "dqlite-specific date/time processors do not consult this "
                "flag (pysqlite's are different processors). Pass dates as "
                "Python ``datetime`` / ``date`` objects directly; the "
                "wire-layer ISO8601 codec round-trips them losslessly."
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
    # Full set of dbapi.connect kwargs the dialect forwards. The URL
    # query path is restricted to the subset in ``_URL_QUERY_ALLOWED``
    # below (typed conversion + range validation); the
    # ``connect_args=`` path bypasses the URL allowlist (SA's
    # ``cparams.union(connect_args)`` happens AFTER
    # ``create_connect_args`` returns), so unknown keys would
    # otherwise reach ``dqlitedbapi.connect`` where they raise
    # ``NotSupportedError`` at first checkout — far from the user's
    # ``create_engine(connect_args=...)`` site. Validate the merged
    # kwarg set inside ``connect()`` against the allowlist below to
    # catch typos at first checkout with the same ``ArgumentError``
    # class the URL path emits at engine construction.
    _CONNECT_KWARG_ALLOWED: frozenset[str] = frozenset(
        {
            "address",
            "database",
            "timeout",
            "max_total_rows",
            "max_continuation_frames",
            "trust_server_heartbeat",
            "close_timeout",
        }
    )

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
        # close_timeout floor: 0.01 s. The dispose-time writer-close
        # is scheduled via ``call_soon_threadsafe`` and joined with a
        # bounded thread.join — a value below 0.01 s gives the loop
        # too few ticks to flush FIN, leaving connections lingering
        # in TIME_WAIT. The validator rejects obviously-useless
        # values up-front so an operator pinning ``?close_timeout=
        # 0.0001`` sees a config-time ArgumentError, not a silent
        # near-zero wait that still appears to "work".
        "close_timeout": (float, lambda v: math.isfinite(v) and v >= 0.01),
    }

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from URL.

        URL format: ``dqlite://host:port/database?timeout=...``

        Known query parameters are typed and range-validated at URL-parse
        time so typos (``?timeoutt=5``), unparseable types
        (``?timeout=abc``), and out-of-range values
        (``?max_total_rows=-1``) all raise :class:`ArgumentError` before
        any pool is built.

        When a query parameter is repeated
        (``?max_total_rows=100&max_total_rows=200``), the **last**
        occurrence wins. SQLAlchemy's URL parser surfaces repeated
        values as a tuple; the dialect uses ``raw[-1]``. This mirrors
        ``urllib.parse.parse_qsl`` ordering and is stable across
        SQLAlchemy versions, but operators templating connection URLs
        from layered config should be aware that a duplicated key
        silently overrides earlier values rather than raising.

        Multi-node bootstrap: the URL format carries a single
        host:port. The dqlite client side will resolve the leader
        starting from that one address (other cluster nodes are
        discovered via the leader-info request), but if the configured
        URL host is itself unreachable, leader-discovery cannot start.
        Operators who need bootstrap-from-many-addresses today should
        use a load-balancer or DNS-RR in front of the cluster, or
        rotate the URL through multiple node addresses across
        deployments. Multi-address bootstrap exposed at the dialect
        level is not part of this surface.
        """
        # Reject any userinfo presence (including bare ``@``, empty
        # username, empty password). The earlier ``or``-truthy guard
        # let ``dqlite://@host`` slip silently because SA's
        # ``make_url`` parses ``username=''``, ``password=None`` (both
        # falsy). Use ``is not None`` so any structural userinfo
        # surface variant — empty or filled — is rejected at parse
        # time. dqlite has no built-in authentication; credentials
        # embedded in the URL would be silently dropped.
        if url.username is not None or url.password is not None:
            raise ArgumentError(
                "Invalid URL: dqlite does not accept username or password in the URL"
            )
        # SA's ``make_url`` does NOT split fragments from queries — a
        # ``?key=value#frag`` URL parses with ``query={'key':
        # 'value#frag'}``, and a fragment without query lands in the
        # database name (``/db#frag`` → ``database='db#frag'``). By
        # the time ``create_connect_args`` runs, the original URL
        # string is no longer accessible (no ``url.fragment``
        # attribute), but the misplaced ``#`` is detectable in those
        # downstream fields. Detect and raise at parse time so the
        # operator sees a clear "fragment" diagnostic rather than the
        # downstream "Cannot convert URL query ..." pointing at the
        # value (or, worse, a database-name with ``#`` silently being
        # used). dqlite URL has no semantic for fragments.
        if url.database is not None and "#" in url.database:
            raise ArgumentError(
                f"Invalid URL: dqlite does not accept URL fragments "
                f"(got database={url.database!r}; '#' likely starts a "
                f"misplaced fragment)"
            )
        if url.query:
            for key, raw in url.query.items():
                raw_str = raw[-1] if isinstance(raw, tuple) else raw
                if isinstance(raw_str, str) and "#" in raw_str:
                    raise ArgumentError(
                        f"Invalid URL: dqlite does not accept URL fragments "
                        f"(got query {key}={raw_str!r}; '#' likely starts a "
                        f"misplaced fragment)"
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

        # IPv6 hosts contain colons; SA's URL parser strips the
        # bracket notation (``[::1]:9001`` → ``host="::1"``,
        # ``port=9001``). The dqlite-client address parser
        # (``_parse_address``) requires brackets to disambiguate
        # ``[host]:port`` from a colon-separated host literal, so
        # re-introduce them here. IPv4 / DNS hostnames cannot contain
        # ``:`` and pass through unchanged.
        address = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        # Pre-validate the host shape at the SA layer so a bad URL
        # surfaces as ArgumentError from create_engine rather than as
        # a deferred InterfaceError from the first checkout. The
        # dbapi-side parse-at-``__init__`` (per the eager-validation
        # fix) covers the deeper-layer check; this is the
        # SA-construction-time parity. ``_client_parse_address`` is a
        # private helper from the client layer; keep the import local
        # so the SA dialect doesn't require it at module-import time.
        from dqliteclient.connection import parse_address as _client_parse_address

        try:
            _client_parse_address(address)
        except ValueError as e:
            raise ArgumentError(f"Invalid dqlite URL host: {e}") from e
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

        # Operator-UX warning: the URL token ``?max_total_rows=none``
        # silently disables the row-count cap that protects clients
        # from a malicious server returning a multi-GB result-set.
        # An operator templating connection URLs from layered config
        # could accidentally stamp ``none`` into ``max_total_rows``
        # without realising they've disabled the defence. Emit a
        # one-shot WARNING per dialect instance at engine creation.
        # This is documented behaviour, not a bug — a logger.warning
        # makes the disabled state observable in operator logs
        # without changing semantics or breaking existing usage.
        if (
            "max_total_rows" in kwargs
            and kwargs["max_total_rows"] is None
            and not self._max_total_rows_disabled_warning_emitted
        ):
            type(self)._max_total_rows_disabled_warning_emitted = True
            logger.warning(
                "dqlite: ``max_total_rows`` cap disabled via URL "
                "(``?max_total_rows=none`` or ``=disabled`` token). The "
                "client will accept arbitrarily-large result-sets from the "
                "server; a malicious or misconfigured peer can exhaust "
                "client memory. Set an explicit positive int unless you "
                "have a specific need for unbounded results."
            )

        return [], kwargs

    def _validate_connect_kwargs(self, kwargs: dict[str, Any]) -> None:
        """Reject any kwarg not in ``_CONNECT_KWARG_ALLOWED`` with
        ``ArgumentError``. Called from ``connect()`` so the merged
        ``cparams`` (URL-derived plus ``connect_args=`` overlay) gets
        the same allowlist check the URL path applies to ``url.query``
        — closing the asymmetry where a typo in ``connect_args=``
        otherwise reaches ``dqlitedbapi.connect`` and raises
        ``NotSupportedError`` at first checkout, far from the user's
        ``create_engine`` site.
        """
        unknown = set(kwargs) - self._CONNECT_KWARG_ALLOWED
        if unknown:
            raise ArgumentError(
                f"Unknown dqlite connect kwarg(s) {sorted(unknown)!r}. "
                f"Allowed: {sorted(self._CONNECT_KWARG_ALLOWED)}. "
                f"Check ``connect_args=`` for typos — the URL query "
                f"path's allowlist applies only to ``?key=value`` URL "
                f"parameters, not to ``connect_args=`` kwargs."
            )

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Create a sync dbapi connection.

        Validate ``cparams`` against ``_CONNECT_KWARG_ALLOWED`` before
        forwarding to ``dqlitedbapi.connect`` so a typo in
        ``create_engine(connect_args={...})`` raises ``ArgumentError``
        with the same diagnostic class the URL path emits, instead of
        deferring to ``dqlitedbapi.connect``'s ``NotSupportedError``
        at first checkout.

        Sync ``creator_fn`` is a **dqlite-private hook** with no
        cross-dialect SA parity. The async sibling
        (``DqliteDialect_aio.connect``) pops ``async_creator_fn``
        which IS the SA-canonical name shared with asyncpg
        (``asyncpg.py:937``) and aiosqlite (``aiosqlite.py:399``).
        The sync side has no equivalent SA-canonical kwarg —
        SA's standard mechanism for injecting a sync custom-factory
        is ``create_engine(creator=...)`` which routes through the
        pool layer and never touches ``dialect.connect()``. The
        ``creator_fn`` kwarg here is offered for symmetry but
        operators should prefer SA's ``creator=`` whenever possible.
        The pop precedes ``_validate_connect_kwargs`` because the
        strict allowlist would otherwise reject the hook key.

        Unlike the async path's two-step (factory → connect) shape,
        the sync dbapi factory ``loaded_dbapi.connect`` returns an
        already-usable Connection (not a connect-pending object), so
        we trust the creator's return value verbatim — no follow-up
        ``connect()`` call. A creator returning a not-yet-connected
        Connection must arrange its own connect prior to return; the
        sync surface has no idempotency contract because there is no
        eager-connect step here.
        """
        creator_fn = cparams.pop("creator_fn", None)
        self._validate_connect_kwargs(cparams)
        if creator_fn is not None:
            return creator_fn(*cargs, **cparams)
        return self.loaded_dbapi.connect(*cargs, **cparams)

    # Drift defence: pin the isolation-level lookup to match the
    # runtime contract that ``set_isolation_level`` actually honours
    # (only SERIALIZABLE). The inherited pysqlite lookup advertises
    # ``READ UNCOMMITTED`` (rejected with the generic "not supported"
    # message) and ``AUTOCOMMIT`` (rejected with the dedicated
    # ``_AUTOCOMMIT_REJECTION_MSG``); the truthful single-level mapping
    # here matches what we actually accept.
    #
    # Note this static surface deliberately diverges from
    # ``get_isolation_level_values`` below, which DOES advertise
    # ``AUTOCOMMIT`` as a diagnostic-routing channel so SA's
    # ``_assert_and_set_isolation_level`` dispatches to our dedicated
    # rejection message rather than its generic
    # "invalid isolation level" ``ArgumentError``. The lookup table is
    # read by SA-internal paths that bypass the values-list (third-
    # party introspection, future refactors); single-source-of-truth
    # there.
    #
    # Value ``0`` mirrors the parent's PRAGMA-style key shape; we never
    # read it because ``set_isolation_level`` is fully overridden.
    _isolation_lookup = util.immutabledict({"SERIALIZABLE": 0})

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

        Note: ``_isolation_lookup`` (pinned above the class methods)
        deliberately diverges — it is the truthful single-level mapping
        SA-internal paths read by-key. The two surfaces serve different
        consumers; see the ``_isolation_lookup`` comment for the why.
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

        Note on ``level=None``: ``DefaultDialect.reset_isolation_level``
        calls this with ``self.default_isolation_level`` which is
        ``"SERIALIZABLE"`` after ``initialize()`` runs — but
        ``initialize()`` may be skipped by test harnesses or by direct
        callers that build a dialect without an engine. Treat
        ``level=None`` as a deliberate no-op (the level is already
        SERIALIZABLE, no-op is correct) rather than raising
        ``ArgumentError``; raising would break the
        ``reset_isolation_level`` path on harnesses that bypass
        ``initialize()``. Diverges from pysqlite's
        ``_isolation_lookup`` which would ``KeyError`` on None.
        """
        if level is None or level == "SERIALIZABLE":
            return
        if level == "AUTOCOMMIT":
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        raise ArgumentError(
            f"dqlite only supports SERIALIZABLE isolation; requested level "
            f"{level!r} is not supported."
        )

    def on_connect(self) -> Callable[[DBAPIConnection], None]:
        """Override pysqlite's ``on_connect`` to a no-op.

        ``SQLiteDialect_pysqlite.on_connect`` returns a callable that
        registers ``regexp`` and ``floor`` user-defined functions via
        ``Connection.create_function``. dqlitedbapi's
        ``Connection.create_function`` raises ``NotSupportedError``
        because the dqlite server has no UDF primitive — every checkout
        would fail at the pysqlite-inherited ``on_connect`` callback.

        Return a no-op callable so SA's connect-event chain has the
        right shape but doesn't touch the connection. (Returning
        ``None`` would also work at the SA runtime layer, but the
        parent class's return annotation pins ``Callable[..., None]``
        and the compliance suite tests cover the calling shape; keep
        the parent's annotation.) If a future dqlite version gains a
        UDF primitive, this is the hook to register replacements at.
        """
        return lambda _conn: None

    def detect_autocommit_setting(self, dbapi_conn: DBAPIConnection) -> bool:
        """The SA dialect always brackets statements in BEGIN / COMMIT.

        The underlying wire protocol is autocommit-by-default and the
        bare dbapi ``Connection.autocommit`` property reports ``True``,
        but SA wraps every statement in a transaction lifecycle so
        from SA's perspective the connection is never in autocommit
        mode. Mirrors ``set_isolation_level``'s rejection of
        ``"AUTOCOMMIT"``.

        The dqlite dbapi ``Connection`` exposes ``isolation_level``
        returning ``None`` (stdlib pre-3.12 parity stub). Without this
        override, SA's pysqlite-style probe
        (``dbapi_conn.isolation_level is None``) would succeed, which
        in turn flips on ``skip_autocommit_rollback`` in
        ``engine/default.py::do_rollback`` ->
        ``engine/base.py:1115-1124``. That short-circuit was designed
        for stdlib sqlite3's connection-level auto-BEGIN, which dqlite
        does NOT implement — every ``engine.begin()`` block would
        bypass our explicit-BEGIN / explicit-COMMIT wire path and
        silently corrupt transaction atomicity.

        Returning ``False`` unconditionally — regardless of what the
        dbapi probe says — keeps ``skip_autocommit_rollback`` disabled
        and preserves the SA-managed BEGIN/COMMIT lifecycle. Do not
        delete this override even if the dbapi-side
        ``isolation_level`` probe later returns the same value the
        parent class would have inferred; the override is the contract
        between the SA dialect and the dqlite dbapi's stdlib-parity
        stubs.
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
        # Async dialect's ``_handle_exception`` remap of
        # ``RuntimeError("Event loop is closed")`` (see
        # ``aio.py``'s remap site). The fault means the asyncio
        # machinery is dead — the slot is permanently unusable
        # and the pool must invalidate.
        "event loop closed",
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
        # ``RuntimeError("This event loop is already running")``
        # surfaces from ``await_only`` inside a context that already
        # has a running loop on the same thread (asyncio rejects
        # nested loop entry). The async adapter's ``_handle_exception``
        # remaps to ``OperationalError("event loop already running:
        # ...")``; the substring ``"loop is already running"``
        # matches the remapped wording without overlapping the other
        # loop-class entries.
        "loop is already running",
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
        ``dqlitedbapi.AsyncConnection._ensure_locks`` raises (and
        which ``AsyncCursor.execute`` propagates). During a real
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
            # OS-level transport faults sitting deeper in the cause
            # chain — wrapped inside a custom retry/middleware layer
            # that catches OSError and re-raises a non-OSError, or the
            # dbapi's own ``OperationalError("Failed to connect:
            # ...") from os_err`` shape. The bare-``e`` arm at the
            # entry of ``is_disconnect`` only catches the direct case;
            # the walk must classify each node by type for symmetry
            # with the DqliteConnectionError / ClusterError direct
            # hit above. Without this, a wrapper-layer OSError silently
            # leaves the slot in the pool and chronically re-fails on
            # every checkout.
            if isinstance(cause, OSError):
                return True
            # Closed-handle InterfaceError surface. Match against
            # ``raw_message`` (un-truncated server text) when present,
            # falling back to ``str(cause)``. Without raw_message
            # priority, a long server message that contains the
            # closed-handle clause beyond the 1 KiB display cap would
            # miss the substring. Mirrors the discipline applied to
            # the OperationalError substring scan below.
            if isinstance(cause, _dbapi_exc.InterfaceError):
                raw = getattr(cause, "raw_message", None) or str(cause)
                message = raw.lower()
                if "connection is closed" in message or "cursor is closed" in message:
                    return True
            # Leader-change code on either OperationalError shape —
            # checked before the substring scan so a coded leader-flip
            # is not gated out by the OE-arm code-is-None restriction
            # below.
            for err_class in (_dbapi_exc.OperationalError, _client_exc.OperationalError):
                if (
                    isinstance(cause, err_class)
                    and getattr(cause, "code", None) in LEADER_ERROR_CODES
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
            elif isinstance(cause, _dbapi_exc.InterfaceError):
                # Server-emitted ``DQLITE_PROTO`` (1001) carries a
                # transport-style server message the substring
                # scanner should classify as disconnect.
                # ``SQLITE_RANGE`` (25) and ``SQLITE_MISUSE`` (21)
                # are caller-side bugs and MUST NOT trigger pool
                # invalidation — retrying them against a fresh
                # connection re-runs the same broken caller code.
                # Restrict the substring scan to the explicit
                # disconnect-eligible code set.
                applies_substring = (
                    getattr(cause, "code", None) in _SERVER_INTERFACEERROR_DISCONNECT_CODES
                )
            else:
                applies_substring = False
            if applies_substring:
                text = getattr(cause, "raw_message", None) or str(cause)
                msg_lower = text.lower()
                for pattern in self._dqlite_disconnect_messages:
                    if pattern in msg_lower:
                        return True
        # Do NOT delegate to ``super().is_disconnect()``. The parent
        # ``SQLiteDialect_pysqlite.is_disconnect`` checks
        # ``isinstance(e, self.dbapi.ProgrammingError) and "Cannot
        # operate on a closed database." in str(e)`` — that's the
        # in-process sqlite3 surface only, and is already subsumed by
        # our broader classifier above. Worse: the super call
        # dereferences ``self.dbapi`` which is ``None`` until SA's
        # ``initialize()`` runs, so test-time invocations on a bare
        # dialect (``DqliteDialect().is_disconnect(...)``) crash with
        # ``AttributeError: 'NoneType' object has no attribute
        # 'ProgrammingError'``.
        return False

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

    def do_terminate(self, dbapi_connection: Any) -> None:
        """Force-close the connection without awaiting in-flight ops.

        SA's pool calls this for forced reclaim during
        ``engine.dispose()`` under failure or shutdown. ``has_terminate
        = True`` (above) promises SA that the path is bounded — unlike
        :meth:`do_close`, which awaits ``Connection._close_async`` for
        up to ``self._timeout`` (default 10 s, gated on a parked wire
        read).

        Routes through the dbapi's :meth:`Connection.force_close_transport`,
        which schedules ``writer.close()`` on the loop thread and
        bounds the thread join with ``close_timeout`` (default 0.5 s).
        Mirrors the async sibling at ``aio.py``.

        ``has_terminate=True`` promises a non-raising path; suppress
        any tail exception so SA's pool finalize cannot crash on a
        partial-state connection (matches the async sibling's
        suppression discipline).
        """
        try:
            dbapi_connection.force_close_transport()
        except Exception:  # noqa: BLE001 - terminate must not raise
            logger.debug(
                "do_terminate: force_close_transport raised; proceeding",
                exc_info=True,
            )

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
        # ``cursor()`` itself can raise on a slot that was reset between
        # checkin and checkout: ``OSError`` (live socket-RST / ECONNRESET)
        # and ``InterfaceError`` ("Connection is closed") are the
        # observable shapes. Both must classify as ping-fail rather
        # than propagating past ``_do_ping_w_event``'s
        # ``loaded_dbapi.Error`` filter — ``OSError`` is NOT a
        # ``dbapi.Error`` subclass and would surface to the SA caller
        # uncaught. Hoisting the call inside the outer try routes both
        # through the same disconnect-classification arm as
        # ``cursor.execute(...)`` failures.
        cursor: Any = None
        try:
            try:
                cursor = dbapi_connection.cursor()
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
                #   * ``InterfaceError`` — closed cursor / connection
                #     AND server-emitted code-bearing variants
                #     (``DQLITE_PROTO`` = 1001 protocol misuse,
                #     ``SQLITE_MISUSE`` = 21, ``SQLITE_RANGE`` = 25;
                #     see ``_SERVER_INTERFACEERROR_DISCONNECT_CODES``
                #     above). For the pre-ping context, ANY
                #     ``InterfaceError`` from ``SELECT 1`` indicates
                #     the slot is unusable — more conservative than
                #     ``is_disconnect``'s real-query classification,
                #     which gates code-bearing ``InterfaceError`` out
                #     so caller-side bind misuse propagates as a real
                #     error. Asymmetry deliberate: pre-ping must
                #     reject any unusable slot; real-query must let
                #     caller-side errors surface.
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
            # close-swallow from close-success in logs. ``cursor`` is
            # ``None`` when ``cursor()`` itself raised — skip the close
            # in that arm.
            if cursor is not None:
                try:
                    cursor.close()
                except (
                    # See the outer ``except`` rationale — same umbrella
                    # so ``cursor.close()`` failures from
                    # CORRUPT/FORMAT/NOTADB are also debug-logged rather
                    # than crashing the ping.
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

    # ``_get_server_version_info`` is inherited from
    # ``SQLiteDialect_pysqlite``. Its one-line implementation
    # (``return self.dbapi.sqlite_version_info``) reads the module-
    # level constant pinned by ``dqlitedbapi`` and lets
    # ``AttributeError`` propagate if a broken DBAPI stub omits the
    # constant — exactly the contract our previous local override
    # encoded. The behaviour is verified by ``TestGetServerVersionInfo``
    # below (no live wire round-trip, value forwards from the dbapi
    # module, attribute-error propagation on stub modules).
    #
    # **Pin contract**: ``dqlitedbapi.sqlite_version_info`` (defined
    # in ``dqlitedbapi/_constants.py``) is the *floor* the dqlite
    # project guarantees, NOT the version this dbapi was developed
    # against. SA gates feature dispatch on this single number — if
    # the cluster runs an *older* SQLite than the floor, SA dispatch
    # produces queries the cluster rejects (RETURNING etc.).
    # Operators must ensure their cluster's SQLite >= the floor.
    # See ``dqlitedbapi/_constants.py`` for the full pin contract.
