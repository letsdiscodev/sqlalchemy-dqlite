"""Base dqlite dialect for SQLAlchemy."""

import contextlib
import datetime
import inspect
import logging
import math
import types
from collections.abc import Callable, Iterator, Sequence
from typing import Any, ClassVar, Final, NoReturn

from sqlalchemy import pool, util
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite.base import SQLiteCompiler
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import BindTyping, DBAPIConnection, IsolationLevel
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.compiler import InsertmanyvaluesSentinelOpts

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from dqliteclient import CLOSE_TIMEOUT_FLOOR, CLOSE_TIMEOUT_FLOOR_RATIONALE, validate_timeout
from dqlitedbapi import FAILED_TO_CONNECT_PREFIX as _DBAPI_FAILED_TO_CONNECT_PREFIX
from dqlitewire import (
    BARE_DATABASE_ERROR_CODES,
    DEFAULT_MAX_CONTINUATION_FRAMES,
    DQLITE_PROTO,
    LEADER_ERROR_CODES,
    LEADER_LOST_DB_LOOKUP_SUBSTRING,
    WIRE_DECODE_FAILED_PREFIX,
)
from dqlitewire import SQLITE_NOTFOUND as _SQLITE_NOTFOUND
from dqlitewire import sanitize_for_log as _sanitize_for_log
from dqlitewire import sanitize_server_text as _sanitize_server_text


def _make_timeout_url_validator(
    field_name: str,
    *,
    min_value: float | None = None,
    min_value_rationale: str | None = None,
) -> Callable[[float], bool]:
    """Build a URL-time timeout validator that delegates to the
    client-layer ``validate_timeout`` SSOT.

    Routes every URL-parse-time timeout check through the same
    invariants the direct ``DqliteConnection`` / ``ConnectionPool``
    callers and the dbapi-layer ``connect_args=`` path use, so any
    future tightening at the client layer flows through automatically
    instead of leaving the SA URL surface as the asymmetric arm.
    Returns ``True`` on success (truthy so the URL dispatcher accepts
    the value). Translates ``ValueError`` / ``TypeError`` from the
    client validator to ``ArgumentError`` so the SA URL-parse contract
    (URL-time errors surface as ``ArgumentError``) is preserved.
    """

    def validator(value: float) -> bool:
        try:
            kwargs: dict[str, Any] = {"name": field_name}
            if min_value is not None:
                kwargs["min_value"] = min_value
            if min_value_rationale is not None:
                kwargs["min_value_rationale"] = min_value_rationale
            validate_timeout(value, **kwargs)
        except (TypeError, ValueError) as e:
            raise ArgumentError(str(e)) from e
        return True

    return validator


_validate_close_timeout_url = _make_timeout_url_validator(
    "close_timeout",
    min_value=CLOSE_TIMEOUT_FLOOR,
    min_value_rationale=CLOSE_TIMEOUT_FLOOR_RATIONALE,
)
_validate_dial_timeout_url = _make_timeout_url_validator("dial_timeout")
_validate_attempt_timeout_url = _make_timeout_url_validator("attempt_timeout")


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
_BARE_DBE_DISCONNECT_CODES: Final[frozenset[int]] = BARE_DATABASE_ERROR_CODES

# URL-time defense-in-depth cap on ``max_continuation_frames``: 10×
# the wire-layer default. The wire-layer's
# ``DEFAULT_MAX_CONTINUATION_FRAMES`` is the operator-facing budget
# that bounds per-RPC continuation-frame decode work; without an
# upper bound on the URL parser, a typo like
# ``?max_continuation_frames=9999999999999999`` is accepted and the
# defense-in-depth ceiling silently collapses. The factor `10` is
# the operator-tunability budget — values up to 10× the default are
# legitimate for high-fanout workloads; anything beyond is a typo
# the URL parser should reject at engine construction. Tying the
# cap to the wire constant via import keeps the relationship intact
# if the wire team ever tunes the default; the prose rationale at
# ``base.py:1340-1349`` documents the 10× choice.
_URL_MAX_CONTINUATION_FRAMES_FACTOR: Final[int] = 10
_URL_MAX_CONTINUATION_FRAMES_CAP: Final[int] = (
    _URL_MAX_CONTINUATION_FRAMES_FACTOR * DEFAULT_MAX_CONTINUATION_FRAMES
)
# **Forward-compat note**: dqlitedbapi's ``_CODE_TO_EXCEPTION`` also
# routes ``SQLITE_NOLFS`` (22), ``SQLITE_AUTH`` (23), ``SQLITE_NOTICE``
# (27), and ``SQLITE_WARNING`` (28) to bare ``DatabaseError``. dqlite-
# server doesn't currently emit any of those four codes on the wire,
# so they are intentionally NOT included here — they are deterministic
# non-transient diagnostics, not slot-fatal conditions. The substring
# scan in ``is_disconnect``'s ``DatabaseError`` arm is gated on the
# code being in this set (``applies_substring`` arm), so it does NOT
# fire for the four excluded codes; if a future server release
# starts emitting them with a transport-class message, classification
# falls through to the substring arms on the wrapping
# ``OperationalError`` / ``InterfaceError`` (the dbapi's
# ``_call_client`` rewrap chain attaches ``raw_message`` and the wire
# layer's ``_sanitize_server_text`` runs first). The
# ``CORRUPT`` / ``FORMAT`` / ``NOTADB`` triad in this set is the
# tight-loop "kill the slot" group — unambiguous regardless of the
# message, and the only codes the server actually emits as bare
# DatabaseError today.

# RAFT-collapse marker phrases for the narrow ``code=1`` substring
# scan. The C dqlite ``translateRaftErrCode`` collapses
# ``RAFT_SHUTDOWN`` / ``RAFT_CANCELED`` / ``RAFT_NOCONNECTION`` (and
# every other non-NOTLEADER / non-LEADERSHIPLOST / non-CANTCHANGE
# raft error) to ``SQLITE_ERROR`` (=1) on the wire, with the verbatim
# ``raft_strerror`` message attached. Without this set, the code=1
# gate at ``is_disconnect``'s OperationalError arm disables the
# substring scan and these cluster-mgmt failures never classify as
# disconnect — leaving SA's pool to keep a torn-state slot. Verified
# against ``dqlite-upstream`` commit ``f30fc99`` (src/translate.c
# and src/raft/err.h). The marker set is intentionally narrow:
# - Whole canonical phrases (not single words) so a user trigger
#   message like ``RAISE(ABORT, 'the system is shutting down')``
#   does not false-positive against ``"server is shutting down"``.
# - ``"i/o error"`` / ``"out of memory"`` are deliberately excluded
#   — too generic; could match legitimate user-side I/O / memory
#   diagnostics surfaced through unrelated code paths.
# Future raft library updates may change wording; the test suite
# pins each marker against a synthesised OperationalError(code=1).
_RAFT_COLLAPSE_DISCONNECT_MARKERS: Final[tuple[str, ...]] = (
    "server is shutting down",
    "operation canceled",
    "no connection to remote server",
)
# ``SQLITE_ERROR`` (==1) is the catch-all primary code used by the
# C ``translateRaftErrCode`` default arm. Not exposed by
# ``dqlitewire.constants`` (it's a generic SQLite primary not a
# dqlite-specific extended code); pinned locally so the
# ``is_disconnect`` gate site reads as intent rather than a literal.
_SQLITE_ERROR_CODE: Final[int] = 1

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

# Suppression set used on BOTH arms of ``do_close``: the first arm's
# ``except _FORCE_CLOSE_TAIL_EXCEPTIONS:`` (so a first-close raise of
# ``RuntimeError("Event loop is closed")`` or ``ReferenceError`` is
# routed through the fallback rather than escaping ``do_close``) and
# the fallback leg's ``contextlib.suppress(*_FORCE_CLOSE_TAIL_EXCEPTIONS)``
# guarding the ``force_close_transport`` call (so the second close
# cannot break the "do_close never raises" invariant either).
# Strictly wider than ``_TRANSPORT_CLASS_EXCEPTIONS``: on the first
# arm the wider tuple keeps loop-state / dead-proxy shapes inside the
# graceful-then-fallback discipline; on the fallback arm the wider
# tuple is the last line of defence for the must-not-raise contract
# when the transport is in an unknown state after the first close
# already failed. Two extra classes vs. the first-close set:
#
# * ``RuntimeError`` — surfaced as ``RuntimeError("Event loop is
#   closed")`` from the dbapi's writer-close machinery during cross-loop
#   ``engine.dispose()``. The async sibling's ``_force_close_transport``
#   absorbs it internally; the sync sibling does not, so the dialect-
#   level suppress is the layer that keeps SA's pool finalize from
#   aborting.
# * ``ReferenceError`` — surfaced when an ``AsyncAdaptedConnection``'s
#   inner ``weakref.proxy`` has been GC'd between the first close and
#   the fallback (rare, but reachable when pytest fixture teardown
#   collects the inner before the dialect's pool-finalize hop). The
#   ``AsyncAdaptedConnection._force_close_transport`` body absorbs
#   ``ReferenceError`` from the public alias, but the sync path through
#   the dbapi ``Connection.force_close_transport`` can still produce one
#   on a half-collected weakproxy on the connection's own _writer slot.
#
# The set is narrow on purpose — ``AttributeError`` / ``TypeError``
# (programmer bugs from a refactor) propagate so real defects do not get
# silently swallowed into the cleanup path.
_FORCE_CLOSE_TAIL_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    *_TRANSPORT_CLASS_EXCEPTIONS,
    RuntimeError,
    ReferenceError,
)

logger = logging.getLogger(__name__)


# Cap server-controlled TEXT cells before they enter %r-formatted log
# lines on the result_processor parse-failure path. The wire layer caps
# each TEXT cell at _MAX_TEXT_VALUE_SIZE = 64 MiB; %r roughly doubles
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


def _log_safe_peer(obj: object) -> str | None:
    """Return ``obj.address`` rendered safe for line-oriented log
    output, or ``None`` if the underlying object exposes no address.

    Routes every interpolation of a peer address through the wire-
    layer ``sanitize_for_log`` discipline. That helper applies
    ``sanitize_server_text`` (strips C0/C1, U+2028/U+2029, full bidi
    block, ZW chars, BOM) AND additionally escapes LF as the literal
    two-byte sequence ``\\n`` and tab as ``\\t`` so a hostile peer
    cannot inject fake log lines into syslog / journald via a
    server-supplied LF in an address field. ``sanitize_server_text``
    alone leaves LF and tab intact for multi-line display rendering;
    that is the wrong contract for ``logger.warning`` /
    ``logger.error`` / ``logger.debug`` interpolation, which is the
    only consumer of this helper.

    Defense-in-depth: the client-layer ``parse_address`` gate rejects
    CRLF / control chars / IDN / credentials-style ``@`` at
    connection-construction time, so today the per-call sanitization
    is a no-op on every in-tree code path. The wrap is still
    load-bearing for two scenarios that bypass the gate:

    1. ``dial_func`` overrides that skip ``parse_address`` and may
       assign a redirect target post-dial (documented bypass at
       ``dqliteclient._dial._dial.py``).
    2. Future refactors that update ``_address`` post-redirect to
       report the connected leader rather than the seed address —
       a reasonable extension whose server-supplied bytes would
       reach the log site without re-validation.

    Mirrors the sibling discipline at ``dqliteclient.connection``'s
    ``_log_safe_address`` (which also uses ``sanitize_for_log``) and
    at every CWE-117-annotated wrap in ``cluster.py`` / ``pool.py``.
    """
    addr = getattr(obj, "address", None)
    if addr is None:
        return None
    return _sanitize_for_log(str(addr))


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


def _walk_cause_chain(
    e: BaseException, max_depth: int = 25, max_nodes: int = 256
) -> Iterator[BaseException]:
    """Yield ``e`` and each ``__cause__`` / ``__context__`` /
    ``BaseExceptionGroup.exceptions`` child up to ``max_depth`` and
    ``max_nodes``.

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

    ``max_nodes=256`` caps the total number of distinct exceptions
    visited so a pathological ``BaseExceptionGroup`` with thousands
    of unique children (constructed by third-party retry middleware
    or fuzzing) cannot drag every ``is_disconnect`` call into O(N)
    work in the group size. 256 is ~10× the in-tree ``_bounded_group``
    cap of 20, which is comfortably above any realistic
    retry+telemetry+circuit-breaker fan-out. Past the cap, the walker
    stops cleanly without raising — the classifier sees the first 256
    nodes (more than enough for any real-world disconnect chain) and
    treats the remainder as opaque.
    """
    from collections import deque

    seen: set[int] = set()
    queue: deque[tuple[BaseException, int]] = deque([(e, 0)])
    while queue:
        if len(seen) >= max_nodes:
            return
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

    **Mixed-writer hazard under** ``DateTime(timezone=False)``.
    SQLAlchemy's ``timezone=False`` column contract is "this column
    stores naive timestamps interpreted as a fixed (usually UTC)
    wall clock." The result processor honours the contract for
    tz-aware wire values (converts via UTC, strips tz) but passes
    naive wire values through UNCHANGED — the same behaviour as
    pysqlite. That's correct IF every writer to the column already
    stored UTC wall clock. dqlite, unlike stdlib ``sqlite3``,
    preserves tz-aware writes on the wire — so a heterogeneous
    client population (e.g. a Go peer writing a local-time naive
    timestamp) can land a naive cell representing local wall clock
    (not UTC) on a column that SA-side readers interpret as UTC.
    The cell is then "wrong by the writer's UTC offset" with no
    diagnostic.

    The dialect does not enforce a write-side UTC discipline; the
    column contract under ``timezone=False`` is the application's
    responsibility to maintain uniformly across every writer to a
    shared cluster. Applications mixing dqlite-SA with native-language
    peers (Go, C, Rust) should either standardise on
    ``DateTime(timezone=True)`` (which converts tz-aware-or-attach-UTC
    on read and avoids the wall-clock-interpretation ambiguity) or
    enforce uniform UTC-wall-clock writes at the application layer.
    Matches pysqlite parity — the divergence is in what wire formats
    the underlying driver allows, not in the SA-side processor
    behaviour.
    """

    # Pysqlite-only kwargs that pysqlite's ``_DateTimeMixin`` /
    # ``DATETIME`` accept and that the dqlite-specific processors
    # do NOT consult. Mirrors the dialect-level ``native_datetime``
    # eager-reject discipline at this type-class layer so the
    # diagnostic surfaces the dqlite divergence directly instead of
    # a bare stdlib ``TypeError`` two frames removed from the
    # ``Column(...)`` call site.
    _DQLITE_REJECTED_KWARGS = ("storage_format", "regexp", "truncate_microseconds")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for kwarg in self._DQLITE_REJECTED_KWARGS:
            if kwarg in kwargs:
                raise ArgumentError(
                    f"dqlite dialect does not honour ``{kwarg}`` on "
                    f"{type(self).__name__}: the dqlite-specific "
                    f"bind/result processors emit a fixed ISO8601 "
                    f"format and parse via "
                    f"``datetime.fromisoformat``; the kwarg would be "
                    f"silently dropped. Use "
                    f"``sqlalchemy.dialects.sqlite.DATETIME`` "
                    f"directly if you need pysqlite's custom storage "
                    f"format — the dqlite colspecs will not adapt "
                    f"that type through. Mirrors the "
                    f"``native_datetime`` eager-reject discipline "
                    f"applied at the dialect level."
                )
        super().__init__(*args, **kwargs)

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
            #
            # Pysqlite's ``_storage_format`` always includes the
            # ``%(microsecond)06d`` fractional component -- the
            # widened midnight datetime serialises as
            # ``"2021-03-15 00:00:00.000000"`` (six trailing zeros),
            # NOT ``"2021-03-15 00:00:00"``. dqlitedbapi's
            # ``_iso8601_from_datetime`` omits the fractional component
            # when ``microsecond == 0``, so going through that encoder
            # would diverge from pysqlite by seven characters. The same
            # asymmetry survived on the non-widen ``datetime`` path
            # below (``return value`` routed plain
            # ``datetime(..., microsecond=0)`` through the encoder and
            # lost the suffix). Both branches now emit the formatted
            # string directly so cross-writer literal-string predicates
            # comparing against pysqlite output match bit-identically.
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                value = datetime.datetime.combine(value, datetime.time())
            if isinstance(value, datetime.datetime):
                base = (
                    f"{value.year:04d}-{value.month:02d}-{value.day:02d} "
                    f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
                    f".{value.microsecond:06d}"
                )
                if value.tzinfo is None:
                    return base
                # Reuse the dbapi-layer offset formatter so the suffix
                # rendering stays in lockstep with the wire codec
                # (whole-minute / ±HH:MM:SS sub-minute handling lives
                # there as a single source of truth).
                offset = value.utcoffset()
                if offset is None:
                    raise _dbapi_exc.DataError(
                        f"DateTime bind: tzinfo {value.tzinfo!r} returned "
                        f"None from utcoffset(); cannot serialise."
                    )
                from dqlitedbapi.types import _format_utc_offset

                return base + _format_utc_offset(offset)
            return value

        return process

    def literal_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        """Render an inline ``DateTime`` literal with always-on six
        fractional digits, mirroring pysqlite's
        ``_storage_format = "...%(microsecond)06d"``
        (``sqlalchemy/dialects/sqlite/base.py``) byte-for-byte.

        The inherited ``sqltypes.DateTime`` literal renderer delegates
        to ``_RenderISO8601NoT`` whose body calls
        ``value.isoformat().replace("T", " ")``; ``datetime.isoformat``
        omits the fractional component when ``microsecond == 0``.
        Pysqlite inherits ``_DateTimeMixin.literal_processor`` which
        builds the literal from the bind processor's storage-format
        string and always emits the suffix. A SQL literal of the form
        ``WHERE col = 'YYYY-MM-DD HH:MM:SS.000000'`` written by a
        pysqlite sibling does NOT match the ``'YYYY-MM-DD HH:MM:SS'``
        form rendered by the inherited path here — the same seven-
        character divergence the bind-side widen-branch fix closed
        for the wire codec.
        """
        bind = self.bind_processor(dialect)
        assert bind is not None

        def process(value: Any) -> str:
            if value is None:
                return "NULL"
            return f"'{bind(value)}'"

        return process

    # ClassVar one-shot gate: fires once per process per type-class so
    # processor churn (SA statement-cache rebuilds, ORM cache
    # evictions, per-query ``text()`` execution) does not re-arm the
    # WARNING. Mirrors the ``_max_total_rows_disabled_warning_emitted``
    # ClassVar pattern on ``DqliteDialect`` (flipped via
    # ``type(self).<attr> = True``). Per-class (not module-global) so
    # a malformed DateTime cell does not silence a malformed Date /
    # Time warning.
    _unparseable_iso_warning_emitted: ClassVar[bool] = False

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
                    if not type(self)._unparseable_iso_warning_emitted:
                        type(self)._unparseable_iso_warning_emitted = True
                        logger.warning(
                            "DateTime processor received unparseable ISO8601 string %r: %s "
                            "(further unparseable rows in this process demoted to DEBUG)",
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

    # Pysqlite-only kwargs that the dqlite-specific processors do NOT
    # consult. ``Date`` has no ``truncate_microseconds`` on pysqlite
    # (it's a ``DATETIME``-only knob). Mirrors the
    # ``native_datetime`` eager-reject discipline.
    _DQLITE_REJECTED_KWARGS = ("storage_format", "regexp")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for kwarg in self._DQLITE_REJECTED_KWARGS:
            if kwarg in kwargs:
                raise ArgumentError(
                    f"dqlite dialect does not honour ``{kwarg}`` on "
                    f"{type(self).__name__}: the dqlite-specific "
                    f"bind/result processors emit a fixed ISO8601 "
                    f"format and parse via "
                    f"``datetime.date.fromisoformat``; the kwarg "
                    f"would be silently dropped. Use "
                    f"``sqlalchemy.dialects.sqlite.DATE`` directly "
                    f"if you need pysqlite's custom storage format "
                    f"— the dqlite colspecs will not adapt that "
                    f"type through. Mirrors the "
                    f"``native_datetime`` eager-reject discipline "
                    f"applied at the dialect level."
                )
        super().__init__(*args, **kwargs)

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

    # ClassVar one-shot gate; see ``_DqliteDateTime`` for rationale.
    _unparseable_iso_warning_emitted: ClassVar[bool] = False

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
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
                    if not type(self)._unparseable_iso_warning_emitted:
                        type(self)._unparseable_iso_warning_emitted = True
                        logger.warning(
                            "Date processor received unparseable ISO8601 string %r: %s "
                            "(further unparseable rows in this process demoted to DEBUG)",
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

    ``bind_processor`` (below) raises ``DataError`` on cross-type
    ``datetime.datetime`` / ``datetime.date`` payloads and formats
    ``datetime.time`` with the always-on six-digit microsecond
    suffix matching pysqlite's ``TIME._storage_format`` byte-for-
    byte. The pysqlite-effective baseline (the SQLite ``TIME`` class
    inherited via ``SQLiteDialect.colspecs``,
    ``sqlalchemy/dialects/sqlite/base.py``) would otherwise reject
    a ``str`` bind with ``TypeError("SQLite Time type only accepts
    Python time objects as input.")``; our override replaces that
    surface with the dqlite-specific bind discipline (PEP 249-shape
    ``DataError`` over ``TypeError``, six-digit microsecond
    formatting for cross-writer parity).
    """

    # Pysqlite-only kwargs that the dqlite-specific processors do NOT
    # consult. Mirrors the ``native_datetime`` eager-reject discipline
    # at the type-class layer.
    _DQLITE_REJECTED_KWARGS = ("storage_format", "regexp", "truncate_microseconds")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for kwarg in self._DQLITE_REJECTED_KWARGS:
            if kwarg in kwargs:
                raise ArgumentError(
                    f"dqlite dialect does not honour ``{kwarg}`` on "
                    f"{type(self).__name__}: the dqlite-specific "
                    f"bind/result processors emit a fixed ISO8601 "
                    f"format and parse via "
                    f"``datetime.time.fromisoformat``; the kwarg "
                    f"would be silently dropped. Use "
                    f"``sqlalchemy.dialects.sqlite.TIME`` directly "
                    f"if you need pysqlite's custom storage format "
                    f"— the dqlite colspecs will not adapt that "
                    f"type through. Mirrors the "
                    f"``native_datetime`` eager-reject discipline "
                    f"applied at the dialect level."
                )
        super().__init__(*args, **kwargs)

    def bind_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            # Cross-type rejection (sibling parity with
            # ``_DqliteDateTime`` / ``_DqliteDate``): the result
            # processor narrows ``datetime.datetime`` lossily via
            # ``.time()`` and would not handle a ``datetime.date``
            # payload at all (returns it verbatim — wrong concrete
            # type on the column). Pre-empt the silent-corruption
            # / round-trip-self-rejection fork by raising at bind.
            # ``isinstance(datetime.datetime)`` ordered before
            # ``datetime.date`` because the former IS the latter.
            if isinstance(value, datetime.datetime):
                raise _dbapi_exc.DataError(
                    f"Time column cannot bind datetime payload "
                    f"{value!r}: the cell would encode as a full "
                    f"ISO8601 timestamp; narrow to "
                    f".time() / .timetz() at the call site."
                )
            if isinstance(value, datetime.date):
                raise _dbapi_exc.DataError(
                    f"Time column cannot bind date payload {value!r}: a date has no time component."
                )
            # Format ``datetime.time`` directly with the always-on
            # six-digit microsecond suffix — pysqlite parity with
            # ``TIME._storage_format = "...%(microsecond)06d"``
            # (``sqlalchemy/dialects/sqlite/base.py``). Without this,
            # ``time(12, 30, 0)`` reaches dqlitedbapi's
            # ``_iso8601_from_time`` which omits the suffix when
            # ``microsecond == 0`` — breaking cross-writer literal-
            # string predicates against pysqlite-written cells.
            # Mirrors the ``_DqliteDateTime.bind_processor`` widen-
            # branch discipline.
            if isinstance(value, datetime.time):
                base = (
                    f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
                    f".{value.microsecond:06d}"
                )
                if value.tzinfo is None:
                    return base
                offset = value.utcoffset()
                if offset is None:
                    raise _dbapi_exc.DataError(
                        f"Time bind: tz-aware time {value!r} "
                        f"returned None from utcoffset(); cannot "
                        f"serialise without a resolvable offset."
                    )
                from dqlitedbapi.types import _format_utc_offset

                return base + _format_utc_offset(offset)
            return value

        return process

    def literal_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        """Render an inline ``Time`` literal with always-on six
        fractional digits, mirroring pysqlite's
        ``TIME._storage_format = "...%(microsecond)06d"``
        (``sqlalchemy/dialects/sqlite/base.py``) byte-for-byte.

        The inherited ``sqltypes.Time`` literal renderer delegates to
        ``_RenderISO8601NoT`` whose body calls
        ``value.isoformat().split("T")[-1]``; ``time.isoformat`` omits
        the fractional component when ``microsecond == 0``. Pysqlite
        inherits ``_DateTimeMixin.literal_processor`` which builds the
        literal from the storage format string and always emits the
        suffix. A SQL literal ``WHERE col = 'HH:MM:SS.000000'`` written
        by a pysqlite sibling does NOT match the ``'HH:MM:SS'`` form
        rendered by the inherited path here — the same seven-character
        divergence the DateTime sibling fix closes.

        Tz-aware ``time`` values render via the offset suffix the
        wire codec uses (``dqlitedbapi._iso8601_from_time``) for
        round-trip parity with how the bind/wire path serialises
        the same value.
        """

        def process(value: Any) -> str:
            if value is None:
                return "NULL"
            if not isinstance(value, datetime.time):
                return f"'{value!s}'"
            base = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"
            if value.tzinfo is None:
                return f"'{base}'"
            # Use the dbapi-layer offset formatter so the suffix
            # rendering stays in lockstep with the wire codec.
            offset = value.utcoffset()
            if offset is None:
                return f"'{base}'"
            from dqlitedbapi.types import _format_utc_offset

            return f"'{base + _format_utc_offset(offset)}'"

        return process

    # ClassVar one-shot gate; see ``_DqliteDateTime`` for rationale.
    _unparseable_iso_warning_emitted: ClassVar[bool] = False

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        want_timezone = self.timezone

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                # Cross-type confusion: ``dqlitedbapi._datetime_from_iso8601``
                # is intentionally polymorphic and decodes a full
                # ``"YYYY-MM-DD HH:MM:SS"`` ISO string into
                # ``datetime.datetime``. If such a payload lands in a
                # ``Time`` column, narrow via ``.timetz()`` — sibling
                # parity with ``_DqliteDate.result_processor``'s
                # ``datetime -> date`` narrowing (the ``value.date()``
                # branch). The date component is silently dropped;
                # ``Time`` has no date dimension to preserve.
                # ``isinstance`` check ordered before
                # ``datetime.time`` because ``datetime.datetime`` is
                # **not** a ``datetime.time`` subclass — both branches
                # need explicit handling.
                #
                # Use ``.timetz()`` rather than ``.time()`` so the
                # source ``tzinfo`` survives the narrowing. The
                # post-narrow value then flows into the
                # ``datetime.time`` branch with its offset intact —
                # ``Time(timezone=True)`` keeps the source offset
                # (no silent UTC rewrite of a non-UTC aware payload)
                # and ``Time(timezone=False)`` strips it for the
                # naive contract. ``.time()`` would have dropped
                # tzinfo unconditionally, and the next branch would
                # then have re-attached UTC to a naive-from-aware
                # value — silently rewriting e.g. an America/Los_Angeles
                # -07:00 offset to +00:00 (7-hour instant shift) with
                # no operator diagnostic.
                value = value.timetz()
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
                    if not type(self)._unparseable_iso_warning_emitted:
                        type(self)._unparseable_iso_warning_emitted = True
                        logger.warning(
                            "Time processor received unparseable ISO8601 string %r: %s "
                            "(further unparseable rows in this process demoted to DEBUG)",
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

    # pysqlite registers a ``floor`` UDF via ``Connection.create_function``
    # on every connect (see ``pysqlite.py::on_connect``) to paper over
    # SQLite builds compiled WITHOUT ``SQLITE_ENABLE_MATH_FUNCTIONS``
    # (the default upstream build flag for SQLite < 3.35). dqlite has
    # no UDF primitive — the SA dialect's ``on_connect`` is a no-op —
    # so ``sa.func.floor(col)`` would silently produce ``no such
    # function: floor`` at runtime on dqlite servers built without
    # math functions.
    #
    # Symmetric with ``visit_regexp_match_op_binary`` above: emit a
    # ``NotSupportedError`` at compile time naming the SQLite build
    # dependency so the failure surfaces with a clear diagnostic.
    # Operators running a dqlite-server with math functions enabled
    # can subclass ``DqliteCompiler`` and override ``visit_function``
    # to relax this gate.
    _FLOOR_NEEDS_MATH_FUNCTIONS_MSG = (
        "sa.func.floor(...) requires the dqlite-server SQLite build to "
        "be compiled with SQLITE_ENABLE_MATH_FUNCTIONS. The dqlite "
        "dialect cannot register the floor UDF the way SQLAlchemy's "
        "pysqlite dialect does (Connection.create_function) because "
        "the dqlite wire protocol has no UDF primitive. Use server-"
        "side CAST (e.g. ``CAST(col AS INTEGER)``) or pre-compute "
        "client-side."
    )

    def visit_function(
        self,
        func: Any,
        add_to_result_map: Any = None,
        **kwargs: Any,
    ) -> str:
        name = getattr(func, "name", "")
        if isinstance(name, str) and name.lower() == "floor":
            raise _dbapi_exc.NotSupportedError(self._FLOOR_NEEDS_MATH_FUNCTIONS_MSG)
        return super().visit_function(func, add_to_result_map, **kwargs)


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

    **DDL keyword-argument prefix is ``sqlite_*``, NOT ``dqlite_*``**.
    SA's ``DialectKWArgs`` keys per-construct dialect kwargs by the
    user-written prefix (the regex on ``<prefix>_<arg>``), so a
    ``Table(..., dqlite_with_rowid=False)`` stores the value under
    ``dialect_options['dqlite']``. The inherited
    ``SQLiteDDLCompiler``, however, reads exclusively from
    ``dialect_options['sqlite']``. Therefore the ``dqlite_*`` form
    is silently dropped at compile time; the ``sqlite_*`` form is
    the one that takes effect.

    **Isolation levels**: only ``SERIALIZABLE`` is honoured (dqlite
    is single-leader Raft, so every commit is serialised). Two
    discoverable surfaces deliberately diverge:

    * ``_isolation_lookup`` is the *truthful* set — ``{"SERIALIZABLE":
      0}``. Third-party introspection consulting this private
      attribute (e.g. SA-internal paths that bypass the values-list,
      future refactors) sees only the level actually honoured.
    * ``get_isolation_level_values()`` returns ``["SERIALIZABLE",
      "AUTOCOMMIT"]``. ``AUTOCOMMIT`` is advertised as a *diagnostic-
      routing channel*: SA's ``_assert_and_set_isolation_level``
      validates against the values-list, accepts ``AUTOCOMMIT``,
      forwards to ``set_isolation_level``, which then raises the
      dedicated ``_AUTOCOMMIT_REJECTION_MSG``. Without that
      advertisement, SA's generic "unknown isolation level" rejection
      would fire BEFORE our dedicated message, hiding the
      dqlite-specific guidance.

    Third-party code performing isolation-level introspection should
    consult ``_isolation_lookup.keys()`` for the truthful set;
    ``AUTOCOMMIT`` from ``get_isolation_level_values()`` is a
    rejection-channel marker, not an accepted level.

    Use ``sqlite_with_rowid``, ``sqlite_autoincrement``,
    ``sqlite_strict``, ``sqlite_on_conflict``, ``sqlite_where``
    (Index) and ``sqlite_on_conflict_*`` (Column) — see SA's
    ``SQLiteDialect.construct_arguments`` for the full list.
    """

    name = "dqlite"

    # One-shot warning gate for ``?max_total_rows=none`` in the URL:
    # emit the row-cap-disabled WARNING once per dialect class, not
    # once per connect / once per engine. ``create_connect_args``
    # flips this to True after the first emit.
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

    # Drift-defence pin matching the async sibling's
    # ``is_async = True`` at aio.py. ``DefaultDialect.is_async = False``
    # is inherited but pinning explicitly surfaces the sync/async
    # split as a documented class-level attribute.
    is_async = False

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

    # SQLite (and dqlite) has no ``SEQUENCE`` primitive — ``CREATE
    # SEQUENCE`` would fail with ``near "SEQUENCE": syntax error``
    # at the server. The inherited ``DefaultDialect.supports_sequences
    # = False`` is currently safe; pin explicitly so a future
    # ``DefaultDialect`` default flip cannot silently emit
    # ``SEQUENCE`` DDL the cluster rejects. Mirrors the
    # drift-defence discipline applied to other inherited-but-pinned
    # capability flags in this class.
    supports_sequences = False

    # SQLite (and dqlite) uses ``INTEGER PRIMARY KEY AUTOINCREMENT``
    # semantics, NOT SQL-standard ``GENERATED BY DEFAULT AS
    # IDENTITY``. The inherited ``DefaultDialect.supports_identity_columns
    # = False`` is currently safe; pin explicitly so an upstream
    # flip can't silently emit identity-column DDL the cluster
    # rejects. Same drift-defence rationale as ``supports_sequences``
    # above.
    supports_identity_columns = False

    # SQLite-style ``INTEGER PRIMARY KEY AUTOINCREMENT`` columns are
    # populated by the server post-INSERT and surfaced via
    # ``last_insert_rowid()`` — there is no pre-execution sequence to
    # fetch. The inherited
    # ``DefaultDialect.preexecute_autoincrement_sequences = False`` is
    # currently safe; pin explicitly so an upstream flip can't trigger
    # spurious pre-execute SELECTs against non-existent sequences.
    preexecute_autoincrement_sequences = False

    # SA's pool gates its forced-disposal path on ``has_terminate``
    # (see ``pool/base.py`` docs for ``_ConnectionRecord.invalidate``
    # and ``DefaultDialect.do_terminate`` line 717). The inherited
    # default is ``False`` (do_terminate falls back to do_close),
    # which would route engine.dispose's forced reclaim through
    # ``Connection.close()`` — bounded by the dbapi connection's
    # ``timeout`` attribute (default 10 s, gated on a parked wire
    # read). Under partition + SIGTERM that 10 s blocks operator
    # shutdown SLAs.
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
    # Sibling drift-defence pins for the rest of SA 2.x's
    # insertmanyvalues machinery and the surrounding `bind_typing` /
    # `supports_for_update_of` flags. DefaultDialect's current values
    # (sqlalchemy/engine/default.py:239-255):
    #   - use_insertmanyvalues_wo_returning = False
    #   - insertmanyvalues_implicit_sentinel = NOT_SUPPORTED
    #   - supports_for_update_of = False
    #   - bind_typing = BindTyping.NONE
    # `insert_executemany_returning_sort_by_parameter_order` is a
    # `util.memoized_property` derived from
    # `insert_returning and use_insertmanyvalues`; with our pins it
    # evaluates to True. Pinning the value explicitly converts the
    # memoized property to a class attribute, surfacing a maintainer-
    # readable contract without changing behaviour (the memoized
    # property cache stores the value on first read; the explicit
    # class attribute is observationally identical).
    use_insertmanyvalues_wo_returning = False
    insertmanyvalues_implicit_sentinel = InsertmanyvaluesSentinelOpts.NOT_SUPPORTED
    supports_for_update_of = False
    insert_executemany_returning_sort_by_parameter_order = True
    bind_typing = BindTyping.NONE
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
    # passthrough for the ``datetime`` path).
    #
    # ``Time`` is also overridden. The pysqlite-effective baseline
    # is the SQLite ``TIME`` class inherited from
    # ``SQLiteDialect.colspecs`` (``sqlalchemy/dialects/sqlite/base.py``)
    # via ``util.update_copy`` — pysqlite does NOT remove the entry
    # and does NOT install its own ``Time`` override. That inherited
    # ``TIME`` calls ``processors.str_to_time`` on the raw cell
    # (``str.fromisoformat`` wrapper) and would raise ``TypeError``
    # on an already-decoded ``datetime.time`` instance that
    # dqlitedbapi returns. Our ``sqltypes.Time: _DqliteTime`` entry
    # replaces that surface with the dqlite-specific bind/result
    # discipline.
    #
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
        # Accept ``paramstyle=None`` as the documented SA "use the dbapi
        # default" sentinel — ``DefaultDialect.__init__(paramstyle=None,
        # ...)`` is the canonical signature and ``None`` resolves via
        # ``self.dbapi.paramstyle`` to ``"qmark"`` in our case. The
        # explicit-``"qmark"`` and ``None``-sentinel cases are the only
        # legitimate inputs; everything else is a misconfiguration.
        ps = kwargs.get("paramstyle")
        if ps is not None and ps != "qmark":
            raise ArgumentError(f"dqlite dialect requires paramstyle='qmark'; got {ps!r}")
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
    # engines) can express the same intent. The continuation-frames
    # ceiling is the dialect's own defense-in-depth cap derived from
    # the wire-layer default — see
    # ``_URL_MAX_CONTINUATION_FRAMES_CAP`` (= 10 ×
    # ``DEFAULT_MAX_CONTINUATION_FRAMES``); the dbapi / wire layers
    # do not enforce a hard ceiling. The ``max_total_rows`` upper
    # of ``2**31 - 1`` is a uint32 protocol invariant (the row-id
    # wire field) rather than a default-derived cap — kept as a hard
    # literal because it tracks the protocol, not a tunable.
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
            # ``max_message_size`` is the wire-layer cap on inbound
            # frame size (default 64 MiB). Accepting on both the
            # ``connect_args=`` and ``_URL_QUERY_ALLOWED`` paths so
            # operators can tune the cap from either SA surface;
            # validation lives at the wire layer (positive int,
            # non-bool — passed through verbatim).
            "max_message_size",
            "trust_server_heartbeat",
            "close_timeout",
            "dial_timeout",
            "attempt_timeout",
            # ``dial_func`` is the dbapi-layer hook for caller-supplied
            # async dialer overrides (per-test process namespaces, IPC
            # sockets, etc.). Accepting it on the ``connect_args=``
            # path lets engine callers inject the hook through SA's
            # standard wiring. The URL-query path is deliberately
            # closed for ``dial_func`` because URL strings cannot
            # carry a callable — typing one in a connection URL is
            # always a typo, so it stays out of ``_URL_QUERY_ALLOWED``
            # below.
            "dial_func",
            # ``busy_timeout`` (seconds, float) — stdlib sqlite3
            # parity. Default ``5.0`` matches sqlite3's C-library
            # default; SA users can override via
            # ``create_engine(connect_args={"busy_timeout": 30.0})``
            # or via the URL form ``?busy_timeout=30.0``. Forwards
            # to ``dqlitedbapi.connect()`` which validates the
            # value (non-negative finite number, non-bool).
            "busy_timeout",
            # ``check_same_thread`` (bool) — stdlib sqlite3 parity.
            # Default ``True`` (dbapi side); set ``False`` to share
            # a Connection across threads. The dialect pins
            # ``pool.QueuePool``; SA's QueuePool checkout/checkin
            # plus the ``_finalize_fairy`` weakref-finalizer (which
            # runs from arbitrary GC threads) both produce cross-
            # thread Connection access patterns. The pool self-
            # heals via ``force_close_transport`` (no _check_thread)
            # even today, but the GC-finalize raise produces a noisy
            # ``logger.error("Exception during reset or similar")``
            # line; passing ``check_same_thread=False`` quiets that
            # and matches the established SA + sqlite3 pattern.
            "check_same_thread",
            # ``begin_immediate`` (bool) — when ``True`` (the dbapi
            # default), bare ``BEGIN`` is rewritten to
            # ``BEGIN IMMEDIATE`` so the writer-lock is acquired up
            # front and the SELECT-then-INSERT pattern can't race a
            # concurrent committer (``SQLITE_BUSY_SNAPSHOT``).
            # Disable with ``connect_args={"begin_immediate": False}``
            # for engines that mostly hold read-only transactions
            # (cuts the writer-lock serialization tax) — but the
            # idiomatic per-session opt-out is the
            # ``dqlite_begin_mode`` execution-option, see
            # ``DqliteDialect.do_begin`` below.
            "begin_immediate",
        }
    )

    # Per-key (converter, validator) tuples. The URL-query path runs
    # converter (str → typed value) then validator (typed → in-range
    # bool); the ``connect_args=`` path skips the converter (callers
    # already pass typed values) and runs only the validator. Because
    # the validator is the single source of truth on the connect_args
    # path, every key MUST carry a non-None validator that fully
    # describes the in-range shape — otherwise connect_args silently
    # bypasses the bound (the converter's range check never runs).
    #
    # The ``not isinstance(v, bool)`` guards exist because ``bool`` is
    # a subclass of ``int`` in Python: ``True > 0`` is True, so a
    # naive ``isinstance(v, int) and 0 < v <= cap`` predicate accepts
    # ``True`` as the integer 1. The URL path can never carry a bool
    # (always converts a string via the named token set), so the
    # bool-rejection is a connect_args-only tightening.
    _URL_QUERY_ALLOWED: dict[str, tuple[Callable[[str], Any], Callable[[Any], bool] | None]] = {
        "timeout": (
            float,
            lambda v: (
                not isinstance(v, bool)
                and isinstance(v, int | float)
                and math.isfinite(v)
                and v > 0
            ),
        ),
        "max_total_rows": (
            lambda s: _parse_url_int_or_none("max_total_rows", s, upper=2**31 - 1),
            lambda v: (
                v is None or (isinstance(v, int) and not isinstance(v, bool) and 0 < v <= 2**31 - 1)
            ),
        ),
        "max_continuation_frames": (
            lambda s: _parse_url_int_or_none(
                "max_continuation_frames", s, upper=_URL_MAX_CONTINUATION_FRAMES_CAP
            ),
            lambda v: (
                v is None
                or (
                    isinstance(v, int)
                    and not isinstance(v, bool)
                    and 0 < v <= _URL_MAX_CONTINUATION_FRAMES_CAP
                )
            ),
        ),
        # ``max_message_size``: wire-layer inbound frame cap.
        # ``None`` is accepted as the dbapi sentinel (= wire-default
        # 64 MiB). The validator's int range mirrors
        # ``max_total_rows`` — positive 32-bit signed; the wire
        # layer revalidates against its own upper bound. Per the
        # reviewer's note, no SA-side cap is imposed beyond the
        # int-shape check: a SA cap would mask legitimate larger
        # values; the protocol layer's existing ValueError is the
        # source of truth.
        "max_message_size": (
            lambda s: _parse_url_int_or_none("max_message_size", s, upper=2**31 - 1),
            lambda v: (
                v is None or (isinstance(v, int) and not isinstance(v, bool) and 0 < v <= 2**31 - 1)
            ),
        ),
        "trust_server_heartbeat": (
            lambda s: _parse_url_bool("trust_server_heartbeat", s),
            lambda v: isinstance(v, bool),
        ),
        # close_timeout floor: 0.01 s. The dispose-time writer-close
        # is scheduled via ``call_soon_threadsafe`` and joined with a
        # bounded thread.join — a value below 0.01 s gives the loop
        # too few ticks to flush FIN, leaving connections lingering
        # in TIME_WAIT. Delegate to the client layer's
        # ``validate_timeout`` so the FIN-flush rationale text reaches
        # the operator pinning ``?close_timeout=0.0001`` in the
        # connection URL — same diagnostic surface as the direct
        # ``DqliteConnection`` / ``ConnectionPool`` callers and the
        # dbapi-layer ``connect_args=`` path. The wrap returns ``True``
        # on success and translates the client-layer ``ValueError`` /
        # ``TypeError`` to ``ArgumentError`` so the SA URL-parse
        # contract (URL-time errors surface as ``ArgumentError``) is
        # preserved.
        "close_timeout": (float, _validate_close_timeout_url),
        # go-dqlite parity knobs: dial_timeout / attempt_timeout
        # mirror Config.DialTimeout / Config.AttemptTimeout on the
        # client layer. Route the URL-time validator through the same
        # ``validate_timeout`` SSOT used by close_timeout so any future
        # client-layer constraint (upper bound, etc.) flows through
        # automatically. Neither knob gates FIN-flush, so the close_
        # timeout floor's rationale does not apply.
        "dial_timeout": (float, _validate_dial_timeout_url),
        "attempt_timeout": (float, _validate_attempt_timeout_url),
        # ``busy_timeout`` (seconds, float) — stdlib sqlite3 parity.
        # Validator mirrors the dbapi-side ``Connection.__init__``
        # check: non-negative (zero accepted, meaning "no retry"),
        # finite, non-bool. The URL converter is ``float`` (per
        # the same shape as ``timeout`` / ``close_timeout``); the
        # validator's accept criterion is the canonical floor
        # because connect_args= bypasses the converter.
        "busy_timeout": (
            float,
            lambda v: (
                not isinstance(v, bool)
                and isinstance(v, int | float)
                and math.isfinite(v)
                and v >= 0
            ),
        ),
        # ``check_same_thread`` (bool) — stdlib sqlite3 parity. The
        # URL parser uses ``_parse_url_bool`` (same as
        # ``trust_server_heartbeat``) to accept ``true``/``false``/
        # ``1``/``0``/``yes``/``no`` case-insensitive. The
        # validator gate on the connect_args= path requires strict
        # bool (rejecting int 0/1 explicitly because
        # ``isinstance(True, int)`` is True and the dbapi-side
        # strict-bool validation would otherwise silently re-fire).
        "check_same_thread": (
            lambda s: _parse_url_bool("check_same_thread", s),
            lambda v: isinstance(v, bool),
        ),
        # ``begin_immediate`` (bool) — see ``_CONNECT_KWARGS_ALLOWED``
        # entry above for the rationale. URL form
        # ``?begin_immediate=false`` matches the same bool-parsing
        # contract as ``check_same_thread`` / ``trust_server_heartbeat``.
        "begin_immediate": (
            lambda s: _parse_url_bool("begin_immediate", s),
            lambda v: isinstance(v, bool),
        ),
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
        # SA-construction-time parity. ``parse_address`` is part of
        # the client's curated top-level surface; the import is kept
        # local so a SA-only environment without ``dqliteclient``
        # installed doesn't fail at module-load time.
        from dqliteclient import parse_address as _client_parse_address

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
                "(``?max_total_rows=none``). The client will accept "
                "arbitrarily-large result-sets from the server; a "
                "malicious or misconfigured peer can exhaust client "
                "memory. Set an explicit positive int unless you have "
                "a specific need for unbounded results."
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

        Also runs the per-key value validator from
        ``_URL_QUERY_ALLOWED`` for any kwarg that's also a URL-query
        knob. ``connect_args`` values are already typed (they bypass
        the URL string-converter step) so only the predicate runs;
        out-of-range values raise the same ``ArgumentError`` class the
        URL path emits at engine construction time. Without this, the
        ``connect_args`` path silently accepts values like
        ``close_timeout=0.0001`` that the URL path rejects on the
        documented 0.01s floor.
        """
        # Surface the dedicated AUTOCOMMIT rejection on the
        # connect_args path, matching the engine-level rejection in
        # ``__init__`` (the ``iso_level.upper() == "AUTOCOMMIT"``
        # branch) that fires for
        # ``create_engine(..., isolation_level="AUTOCOMMIT")``. Without
        # this special-case, the rejection still happens via the
        # allowlist below, but with a generic "Unknown dqlite connect
        # kwarg" message that gives operators no hint that the
        # rejection is by-design rather than a typo. Mirror the
        # engine-level guard's case-insensitive uppercase compare and
        # ``isinstance(..., str)`` shape exactly.
        iso_level = kwargs.get("isolation_level")
        if isinstance(iso_level, str) and iso_level.upper() == "AUTOCOMMIT":
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        # Non-AUTOCOMMIT ``isolation_level`` in ``connect_args`` is
        # also a real shape (a user typing ``connect_args={
        # "isolation_level": "SERIALIZABLE"}``) — it falls through the
        # generic allowlist below with a misleading "Check
        # ``connect_args=`` for typos" message that points the user
        # the wrong way. The dialect routes ``isolation_level`` only
        # via the engine-level kwarg
        # (``create_engine(isolation_level=...)``); make that explicit
        # with a directional message symmetric with the AUTOCOMMIT
        # arm above.
        if "isolation_level" in kwargs:
            raise ArgumentError(
                "dqlite's SQLAlchemy dialect routes ``isolation_level`` "
                "via ``create_engine(isolation_level=...)``, not "
                "``connect_args``. Move the value to the engine-level "
                "kwarg."
            )
        unknown = set(kwargs) - self._CONNECT_KWARG_ALLOWED
        if unknown:
            raise ArgumentError(
                f"Unknown dqlite connect kwarg(s) {sorted(unknown)!r}. "
                f"Allowed: {sorted(self._CONNECT_KWARG_ALLOWED)}. "
                f"Check ``connect_args=`` for typos — the URL query "
                f"path's allowlist applies only to ``?key=value`` URL "
                f"parameters, not to ``connect_args=`` kwargs."
            )
        for key, value in kwargs.items():
            if key in self._URL_QUERY_ALLOWED:
                _converter, validator = self._URL_QUERY_ALLOWED[key]
                if validator is not None and not validator(value):
                    raise ArgumentError(
                        f"connect_args value for {key!r} = {value!r} fails "
                        f"the validator the URL-query path enforces"
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
        if creator_fn is not None:
            # Mirror the async sibling's discipline at ``aio.py``:
            # reject non-callables and async-def shapes here with a
            # precise ``ArgumentError`` rather than letting Python's
            # call-protocol leak ``TypeError`` (non-callable) or
            # silently returning a coroutine from ``connect()``
            # (async-def). The async dialect rejects an async
            # ``creator_fn`` analogously for the inverse mismatch
            # (sync hook on the async URL form).
            if not callable(creator_fn):
                raise ArgumentError(
                    f"connect_args['creator_fn'] must be callable; got {type(creator_fn).__name__}"
                )
            if inspect.iscoroutinefunction(creator_fn):
                raise ArgumentError(
                    "connect_args['creator_fn'] must NOT be async (the sync "
                    "URL form ``dqlite://...`` requires a synchronous hook). "
                    "Use the async URL form ``dqlite+aio://...`` with "
                    "connect_args['async_creator_fn'] instead."
                )
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

    def set_isolation_level(self, dbapi_connection: DBAPIConnection, level: IsolationLevel) -> None:
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

        Note on ``level=None``: pysqlite's parent
        ``SQLiteDialect.set_isolation_level`` raises ``KeyError`` on
        ``None`` (its body is ``self._isolation_lookup[level]``).
        The dqlite dialect mirrors that strict-rejection by raising
        ``ArgumentError`` — the SA-internal reset path that prompted
        the earlier silent-accept-None arm (``reset_isolation_level``
        with ``self.default_isolation_level=None`` on a dialect that
        skipped ``initialize()``) no longer reaches this method,
        because ``reset_isolation_level`` is now overridden to a
        no-op (see the sibling method below). Strict-reject removes
        the divergence from the pysqlite-parity surface.
        """
        # Compare case-insensitively. SA's engine flow upper-cases
        # before dispatching here, so the upper-case form is what
        # production code delivers — but direct callers (test
        # harnesses, custom engine implementations, third-party
        # connect-listener authors) often invoke
        # ``dialect.set_isolation_level(conn, "serializable")``
        # straight. The previous case-sensitive comparison fell
        # through such inputs to the generic rejection message, which
        # is confusing because the value only differs in case.
        if level is None:
            raise ArgumentError(
                "dqlite set_isolation_level requires a non-None level "
                "(use 'SERIALIZABLE' to confirm the dialect's only "
                "accepted level, or 'AUTOCOMMIT' to route through the "
                "dedicated rejection message). The earlier silent-"
                "accept-None arm protected a stale reset_isolation_level "
                "path that no longer reaches this method — reset is now "
                "a local no-op. Mirrors pysqlite's parent KeyError-on-"
                "None behaviour with an SA-shaped ArgumentError."
            )
        if not isinstance(level, str):
            raise ArgumentError(
                f"dqlite only supports SERIALIZABLE isolation; requested level "
                f"{level!r} is not a string."
            )
        normalised = level.upper()
        if normalised == "SERIALIZABLE":
            return
        if normalised == "AUTOCOMMIT":
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        raise ArgumentError(
            f"dqlite only supports SERIALIZABLE isolation; requested level "
            f"{level!r} is not supported."
        )

    def reset_isolation_level(self, dbapi_connection: DBAPIConnection) -> None:
        """SA pool-checkin hook — no-op on this dialect.

        dqlite is a single-isolation-level engine (SERIALIZABLE; see
        ``get_isolation_level`` / ``set_isolation_level``). There is
        no per-connection isolation state to reset on pool checkin.
        SA's inherited ``DefaultDialect.reset_isolation_level``
        (``engine/default.py:1004-1019``) calls
        ``_assert_and_set_isolation_level`` which validates against
        ``get_isolation_level_values()`` — our values list advertises
        ``"AUTOCOMMIT"`` as a diagnostic-routing channel (see
        ``_isolation_lookup`` docstring above for the divergence
        rationale). If a caller used
        ``Connection.execution_options(isolation_level="AUTOCOMMIT")``
        on a slot, SA's inherited reset would route checkin through
        ``set_isolation_level("AUTOCOMMIT")`` which raises
        ``_AUTOCOMMIT_REJECTION_MSG`` — from a finalize path the user
        did not initiate. SA's pool marks the slot bad and logs a
        warning even though the user's AUTOCOMMIT attempt should have
        been rejected at the ``execution_options`` site (and was).
        Overriding to a no-op keeps the dialect's SERIALIZABLE-only
        contract symmetric across the connect / set / reset surfaces
        and prevents the surprise raise from a SA-internal
        finalize path.

        The override mirrors the discipline applied to the sibling
        ``_isolation_lookup`` truthful-set + values-list-diagnostic-
        channel pattern.
        """

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

        **Foreign-key enforcement is NOT enabled by this hook.**
        SA's SQLite reflection docs (see
        ``.../sqlalchemy/dialects/sqlite/base.py``'s "Foreign Key
        Support" prose) require every connection to issue
        ``PRAGMA foreign_keys = ON`` before use — pysqlite does not
        emit it from ``on_connect`` either; the recipe lives in
        user code as a ``@event.listens_for(engine, "connect")``
        handler. dqlite inherits the same default by design (pysqlite
        parity). Applications that need FK enforcement should attach
        the recipe at engine-construction time, e.g.::

            from sqlalchemy import event


            @event.listens_for(engine, "connect")
            def _fk_pragma_on_connect(dbapi_connection, _):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        The dialect deliberately stays out of this choice so
        applications can opt in or out without a URL knob to manage.
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

    # ``dqlite_begin_mode`` execution-option plumbing.
    #
    # SA's ``Connection.execution_options(dqlite_begin_mode=X)`` /
    # ``Engine.execution_options(dqlite_begin_mode=X)`` route through
    # the dialect's ``set_connection_execution_options`` /
    # ``set_engine_execution_options`` hooks below. Both stash the
    # mode on the underlying dbapi connection so the subsequent
    # ``do_begin`` call (which only receives the dbapi connection,
    # not the SA Connection) can read it.
    #
    # Accepted values: ``"immediate"`` (default, writer-safe),
    # ``"deferred"`` (legacy DEFERRED — vulnerable to SNAPSHOT,
    # opt-in for read-only sessions), ``"exclusive"`` (stronger
    # lock — blocks readers too). Other values raise at
    # ``do_begin`` time.
    _VALID_DQLITE_BEGIN_MODES: ClassVar[frozenset[str]] = frozenset(
        {"immediate", "deferred", "exclusive"}
    )

    @staticmethod
    def _unwrap_dqlite_connection(dbapi_connection: Any) -> Any:
        """Return the underlying ``dqlitedbapi.Connection`` /
        ``AsyncConnection`` from whatever SA hands us.

        For sync engines, ``dbapi_connection`` IS the dqlitedbapi
        Connection (has ``__dict__``). For async engines,
        ``dbapi_connection`` is ``AsyncAdaptedConnection`` which uses
        ``__slots__`` and wraps the real connection at
        ``._connection``. Tooling that wants to stash a flag for the
        dialect to read later (here: ``_dqlite_begin_mode``) needs to
        target the dqlitedbapi side so the flag actually has a place
        to live and the dbapi-layer ``do_begin`` reader sees it.
        """
        inner = getattr(dbapi_connection, "_connection", None)
        return inner if inner is not None else dbapi_connection

    def _validate_dqlite_begin_mode(self, mode: object) -> str:
        """Coerce + validate the ``dqlite_begin_mode`` execution-option
        value, returning the canonical lowercase form. Raise
        ``ArgumentError`` for anything outside the accepted set so
        misuse surfaces at ``execution_options(...)`` call time
        rather than at the first ``do_begin`` after a long-running
        engine has been built."""
        from sqlalchemy.exc import ArgumentError

        if not isinstance(mode, str):
            raise ArgumentError(f"dqlite_begin_mode must be a str, got {type(mode).__name__}")
        normalised = mode.lower()
        if normalised not in self._VALID_DQLITE_BEGIN_MODES:
            raise ArgumentError(
                f"Invalid dqlite_begin_mode {mode!r}; "
                f"valid values are {sorted(self._VALID_DQLITE_BEGIN_MODES)}"
            )
        return normalised

    def set_connection_execution_options(self, connection: Any, opts: Any) -> None:
        """Apply per-connection execution options. Extends the parent
        hook to honour the ``dqlite_begin_mode`` knob by stashing the
        value on the underlying dqlitedbapi Connection (where the
        dbapi-layer ``do_begin`` reader picks it up). All other
        options route through the inherited implementation."""
        super().set_connection_execution_options(connection, opts)
        mode = opts.get("dqlite_begin_mode")
        if mode is not None:
            normalised = self._validate_dqlite_begin_mode(mode)
            target = self._unwrap_dqlite_connection(connection.connection.dbapi_connection)
            target._dqlite_begin_mode = normalised

    def set_engine_execution_options(self, engine: Any, opts: Any) -> None:
        """Apply engine-wide execution options. Extends the parent
        hook to register an ``engine_connect`` listener that stashes
        the ``dqlite_begin_mode`` on every checked-out connection.
        Mirrors SA's own ``connection_characteristics`` pattern."""
        super().set_engine_execution_options(engine, opts)
        mode = opts.get("dqlite_begin_mode")
        if mode is not None:
            normalised = self._validate_dqlite_begin_mode(mode)
            from sqlalchemy import event as _sa_event

            @_sa_event.listens_for(engine, "engine_connect")
            def _stash_dqlite_begin_mode(conn: Any) -> None:
                target = self._unwrap_dqlite_connection(conn.connection.dbapi_connection)
                target._dqlite_begin_mode = normalised

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
    # ROLLBACK a no-op).
    #
    # **BEGIN form selection.** The dialect emits plain ``BEGIN`` by
    # default; the dbapi cursor layer rewrites it to ``BEGIN IMMEDIATE``
    # via ``_pragma_intercept.try_rewrite_begin_to_immediate`` (the
    # writer-lock is acquired up front so the SELECT-then-INSERT
    # pattern cannot lose its snapshot to a concurrent committer —
    # otherwise the INSERT surfaces ``SQLITE_BUSY_SNAPSHOT (517)``
    # which is unrecoverable without a transaction restart).
    # Concurrent ``BEGIN IMMEDIATE``s contend at the writer-lock and
    # surface as ordinary ``SQLITE_BUSY (5)`` which the existing
    # busy_timeout retry absorbs transparently — the stdlib-SQLite
    # contract callers expect. dqlite-upstream's VFS itself
    # recommends ``BEGIN IMMEDIATE`` for write-bearing transactions.
    #
    # Per-session opt-out for explicitly read-only transactions: SA
    # users set the ``dqlite_begin_mode`` execution-option via
    # ``engine.execution_options(dqlite_begin_mode="deferred")`` or
    # the per-connection equivalent. Accepted values:
    #
    #   - ``"immediate"`` (default — writer-safe, see above)
    #   - ``"deferred"`` (legacy SQLite semantics — read-snapshot
    #     opened lazily, vulnerable to SNAPSHOT under contention)
    #   - ``"exclusive"`` (stronger lock — blocks other readers too)
    #
    # ``do_begin`` reads the option from the SA Connection that owns
    # the begin call (via ``dbapi_connection.info``), substitutes the
    # explicit literal, and emits it. Explicit literals
    # (``BEGIN IMMEDIATE`` / ``BEGIN EXCLUSIVE``) bypass the dbapi
    # rewrite. The dbapi off-switch
    # (``connect_args={"begin_immediate": False}``) disables the
    # rewrite for the engine wholesale — for callers who prefer the
    # legacy DEFERRED default.
    #
    # Errors propagate unwrapped — SA's Connection._begin_impl wraps the
    # call in _handle_dbapi_exception, so is_disconnect classification
    # and pool-invalidation kick in for transport-level BEGIN failures.
    #
    # **Asymmetric with the SAVEPOINT family**: ``do_savepoint`` /
    # ``do_release_savepoint`` / ``do_rollback_to_savepoint`` are
    # inherited from ``DefaultDialect`` and route through SA's
    # ``connection.execute(SavepointClause(name))`` pipeline rather
    # than the bespoke raw-cursor shape used here. The pipeline path
    # is correct for the SAVEPOINT case — SA's
    # ``_handle_dbapi_exception`` already wraps any raised exception
    # through ``is_disconnect`` classification before SA propagates,
    # so the close-after-BEGIN-exception-preservation discipline
    # this method enforces is not needed there (SA's pipeline never
    # has a separate close step that could mask the dbapi-raised
    # exception). The asymmetry is intentional; do NOT mirror this
    # raw-cursor shape to the savepoint family.
    def do_begin(self, dbapi_connection: DBAPIConnection) -> None:
        # ``_dqlite_begin_mode`` is stashed on the underlying
        # dqlitedbapi Connection by
        # ``set_connection_execution_options`` /
        # ``set_engine_execution_options`` above when the SA user sets
        # ``execution_options(dqlite_begin_mode="...")``. Missing or
        # ``"immediate"`` (the default) emits bare ``BEGIN`` which the
        # dbapi cursor rewrites to ``BEGIN IMMEDIATE``. ``"deferred"``
        # / ``"exclusive"`` emit the explicit literal which passes
        # through the dbapi rewrite unchanged (caller intent).
        target = self._unwrap_dqlite_connection(dbapi_connection)
        # Read the per-connection mode hint. Only honour a known str
        # literal; any other shape (MagicMock auto-spawn in unit-test
        # fixtures, accidental wrong type slipped past the
        # execution_options validator, ...) falls back to the default
        # "immediate". The strict ArgumentError validation already
        # ran at execution_options time; do_begin is the wrong place
        # to surface late misuse.
        raw_mode = getattr(target, "_dqlite_begin_mode", "immediate")
        mode = raw_mode.lower() if isinstance(raw_mode, str) and raw_mode else "immediate"
        if mode == "deferred":
            begin_sql = "BEGIN DEFERRED"
        elif mode == "exclusive":
            begin_sql = "BEGIN EXCLUSIVE"
        else:
            # ``"immediate"`` (the default) AND any unknown value
            # fall here so a future un-validated typo still produces
            # a working BEGIN rather than a hard error from inside
            # ``do_begin``. Bare BEGIN — the dbapi cursor's rewrite
            # (default on) turns this into ``BEGIN IMMEDIATE`` over
            # the wire. If the dbapi-side rewrite is disabled
            # (``connect_args={"begin_immediate": False}``), bare
            # ``BEGIN`` is sent verbatim — restoring the legacy
            # DEFERRED semantics across the whole engine.
            begin_sql = "BEGIN"
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(begin_sql)
        finally:
            # Wrap close in a narrow defensive block so a transport-
            # class failure here (leader flip mid-BEGIN, dead socket
            # post-BEGIN) does not mask the BEGIN-time exception.
            # Python's ``finally`` clause replaces the currently-
            # propagating exception with any new exception raised
            # inside the finally body — the BEGIN exception would
            # be demoted to ``__context__`` of the close-time error.
            # Although ``is_disconnect``'s ``_walk_cause_chain``
            # at base.py:329-393 DOES walk ``__context__`` (and
            # could in principle recover the BEGIN error), we do not
            # want to depend on that recovery path: the BEGIN error
            # must be the propagating exception so SA's exception
            # wrapping sees it directly, not as a chained
            # ``__context__``.
            try:
                cursor.close()
            except _FORCE_CLOSE_TAIL_EXCEPTIONS:
                # Widened from ``_TRANSPORT_CLASS_EXCEPTIONS`` to the
                # ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` tuple used by
                # ``do_close`` at the same surface — cursor / connection
                # close under cross-loop dispose or dead-``weakref.proxy``
                # state. The wider tuple covers the same two extra shapes
                # ``do_close`` documents: ``RuntimeError("Event loop is
                # closed")`` from cross-loop dispose and ``ReferenceError``
                # from a dead proxy. Under the narrower tuple, those two
                # escaped the ``finally`` body and masked the BEGIN-time
                # exception per Python's "finally replaces propagating
                # exception" rule. ``IntegrityError`` (implausible from
                # close — no constraints fire), programmer-bug shapes
                # (``AttributeError`` / ``TypeError``), and any other
                # ``dbapi.Error`` subclass remain outside the wider
                # tuple and still escape so the bug surfaces rather
                # than masking the BEGIN-time exception silently.
                logger.debug(
                    "do_begin: cursor.close failed after BEGIN; BEGIN exception preserved",
                    exc_info=True,
                )

    def do_executemany(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any = None,
    ) -> None:
        """Intentional opt-out of any future ``DefaultDialect.do_executemany`` growth.

        SA's default at ``engine/default.py:948-952`` is a one-line
        pass-through ``cursor.executemany(statement, parameters)``;
        pysqlite and aiosqlite inherit it unchanged. This override is
        byte-equivalent to that body today, but the override itself
        is the contract: if SA's ``DefaultDialect.do_executemany``
        ever grows wrapper logic (per-parameter-set serialisation
        for a DBAPI quirk on another driver, dispatch-event hooks,
        paramstyle conversion), the dqlite dialect deliberately does
        NOT pick up the new behaviour. The surrounding rowcount /
        executemany flags
        (``supports_sane_multi_rowcount = True``,
        ``insert_executemany_returning = True``) are value-pinned
        locally; this override is the matching behavioural opt-out.

        Limits of what this override delivers:

        * The override does NOT call ``super().do_executemany(...)``,
          so any future hook SA dispatches at the default site
          (e.g. ``dispatch.do_executemany``) is bypassed silently
          on dqlite. To pick up a future SA default extension,
          delete this override or route through ``super()``.
        * The behavioural pin guards against SA inserting wrapper
          logic above ``cursor.executemany``. It does NOT guard
          against the dbapi-layer ``executemany`` itself changing
          semantics — that's a separate contract surface owned by
          ``dqlitedbapi``.
        * The body MUST stay byte-equivalent to the pass-through —
          a behavioural pin test (``test_do_executemany_local_override_pin``)
          asserts exactly one ``cursor.executemany(statement,
          parameters)`` call with the verbatim arguments.

        Any future maintainer who wants the SA-default behaviour back
        (e.g. to opt in to a new SA hook) must delete this override
        deliberately — there is no implicit-inherit path.
        """
        cursor.executemany(statement, parameters)

    def do_execute(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any = None,
    ) -> None:
        """Intentional opt-out of any future ``DefaultDialect.do_execute`` growth.

        Sibling of :meth:`do_executemany` — see that docstring for the
        full drift-defence rationale. SA's three default execute hooks
        (``do_executemany`` / ``do_execute`` / ``do_execute_no_params``)
        at ``engine/default.py:948-955`` are identical-shaped one-liners
        sharing the same evolution surface (future dispatch event,
        envelope tracer, paramstyle conversion). Pinning one but
        inheriting the others would silently pick up any of those
        future wrapper layers — exactly the failure mode the
        ``do_executemany`` override goes to lengths to prevent.

        Body MUST stay byte-equivalent to SA's pass-through. A pin
        test (``test_do_execute_local_override_pin``) drives a stub
        cursor and asserts exactly one ``cursor.execute(statement,
        parameters)`` call with the verbatim arguments.
        """
        cursor.execute(statement, parameters)

    def do_execute_no_params(
        self,
        cursor: Any,
        statement: str,
        context: Any = None,
    ) -> None:
        """Intentional opt-out of any future ``DefaultDialect.do_execute_no_params`` growth.

        Sibling of :meth:`do_executemany` — see that docstring for the
        full drift-defence rationale. Body MUST stay byte-equivalent
        to SA's one-line pass-through. A pin test
        (``test_do_execute_local_override_pin``) drives a stub cursor
        and asserts exactly one ``cursor.execute(statement)`` call
        with the verbatim argument.
        """
        cursor.execute(statement)

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
        # Derived from the dbapi-layer ``FAILED_TO_CONNECT_PREFIX``
        # constant so a future wording change on either side stays
        # in lockstep. The matcher compares case-insensitively
        # against the lowercase, colon-trimmed form of the prefix.
        _DBAPI_FAILED_TO_CONNECT_PREFIX.rstrip(": ").lower(),
        "not connected",
        # Wire-layer desync: ProtocolError / DecodeError surface here
        # via the dbapi wrap at ``cursor._call_client`` that routes
        # ``client.ProtocolError`` to ``OperationalError(code=None)``.
        # The canonical prefix is the shared ``WIRE_DECODE_FAILED_PREFIX``
        # constant from ``dqlitewire``. Producer call sites (5 across
        # 2 packages):
        #   - ``dqliteclient/protocol.py``: 3 sites (StmtResponse
        #     db_id-drift rewrap; top-level decode-failure rewrap in
        #     ``_read_message``; continuation decode-failure rewrap in
        #     ``_read_response``).
        #   - ``dqliteclient/connection.py``: 1 site (handshake-time
        #     ``DqliteConnectionError`` rewrap).
        #   - ``dqlitedbapi/connection.py``: 1 site (connect-path
        #     ``OperationalError`` rewrap).
        # Recognition site (consumer): this tuple entry. The reference
        # in ``dqlitedbapi/cursor.py`` is a documentary comment only —
        # cursor.py propagates the upstream message verbatim and does
        # not emit a new prefix. Renaming the constant ripples through
        # grep into the five producers and this consumer.
        WIRE_DECODE_FAILED_PREFIX,
        # Cross-loop misuse: every path that produces a code=None
        # OperationalError mentioning a loop mismatch routes through
        # ``aio._handle_exception``, which remaps the original
        # ``RuntimeError("<Future ... attached to a different loop>")``
        # / ``ProgrammingError("...different event loop")`` to
        # ``OperationalError(f"event-loop mismatch: {msg}",
        # code=None)``. The canonical prefix is the durable
        # signal — the bare ``"different loop"`` /
        # ``"different event loop"`` substrings the remap reads off
        # the original error are not load-bearing here, and matching
        # them directly would false-positive against a benign user
        # trigger message that happens to mention the same phrase.
        "event-loop mismatch:",
        # ``RuntimeError("This event loop is already running")``
        # surfaces from ``await_only`` inside a context that already
        # has a running loop on the same thread (asyncio rejects
        # nested loop entry). The async adapter's ``_handle_exception``
        # remaps to ``OperationalError("event loop already running:
        # <appended str(hop)>")``. Both substrings are kept for
        # refactor-safety:
        # - ``"event loop already running"`` matches the remap's
        #   emitted prefix verbatim. This is the load-bearing match
        #   for any future refactor that drops / sanitises the
        #   appended ``str(hop)`` cause text — without this entry, a
        #   wording-only refactor of the remap would silently break
        #   disconnect classification with no test catching it.
        # - ``"loop is already running"`` matches the appended
        #   ``str(hop)`` cause text from the original
        #   ``RuntimeError("This event loop is already running")``
        #   for forward-compat with third-party RuntimeErrors that
        #   leak this wording through other code paths.
        "event loop already running",
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
        ``"event-loop mismatch:"`` substring branch picks it up.
        Without that remap the slot would survive a cross-loop fault
        and the next checkout would hit it again.
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
        # connect refused, connection timeout) are classified by the
        # cause-chain walk below. ``_walk_cause_chain`` yields ``e``
        # at depth 0, so a bare OSError is caught by the walk's
        # per-node ``isinstance(cause, OSError)`` arm without a
        # separate early-return here. Keeping the walk as the single
        # gate also handles ``BaseExceptionGroup`` containing OSError
        # children — a case the prior early-return missed because
        # groups are not OSError instances.
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
        # ``message`` argument at ``_DEFAULT_MAX_RAW_MESSAGE`` (4 KiB,
        # the wire-layer SSOT) at construction time — the same cap it
        # applies to ``raw_message``. The client layer applies a
        # stricter 1 KiB ``_MAX_DISPLAY_MESSAGE`` cap on its own
        # user-facing message but that cap is invisible to this
        # classifier path because the substring scan reads from the
        # dbapi-level exception SA hands us. A disconnect substring
        # past byte 4096 in the original server text would otherwise
        # be invisible to ``str(cause)`` (which returns the truncated
        # ``args[0]``); the priority-read of ``raw_message`` covers
        # the full server text within the wire-layer FailureResponse
        # limit (~64 KiB), bounded by the 4 KiB ``raw_message`` budget.
        # Restrict the substring scan to (a) ``OperationalError`` (the
        # historical surface — wire-decode/transport failures) and (b)
        # bare ``DatabaseError`` with codes 11/24/26 (CORRUPT / FORMAT /
        # NOTADB) — the codes that motivated widening the classifier
        # beyond ``OperationalError`` to ``DatabaseError``. Without the
        # code-restriction on the DatabaseError branch, a server-supplied
        # user-defined error message inside an ``IntegrityError`` (e.g.
        # ``RAISE(ABORT, '...timed out validating peer')``) would match
        # the loose ``"timed out"`` substring and be classified as a
        # disconnect.
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
            # ``client.ProtocolError`` is the wire-layer desync class
            # that the dbapi cursor wraps as ``OperationalError(code=
            # None, message="wire decode failed: ...")`` — but the
            # SA-async-adapter at ``aio.py`` imports client classes
            # directly, so a third-party caller that bypasses dbapi
            # (or middleware that catches and re-wraps) can surface a
            # bare ``client.ProtocolError`` deeper in the cause chain.
            # Without this arm, the bare ProtocolError walks past every
            # type-check and the substring scan only fires on the
            # OperationalError-wrapped form. Classify it as disconnect
            # so the SA pool invalidates the slot.
            if isinstance(cause, _client_exc.ProtocolError):
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
                # The dbapi raises ``InterfaceError("DqliteConnection
                # is bound to a closed event loop. ...")`` (via
                # ``dqliteclient.connection`` line 2207) when the
                # original asyncio loop the connection was bound to
                # has been GC'd. The slot is permanently dead — the
                # weakref-bound loop cannot be revived. SA's pool
                # MUST invalidate so the next acquire rebuilds in
                # the current live loop. Symmetric raise at
                # ``dqliteclient.connection`` line 2212 surfaces
                # ``InterfaceError("DqliteConnection is bound to a
                # different event loop. ...")`` when a still-bound
                # slot is reused from a different loop in the same
                # thread (e.g., after a prior ``asyncio.run(...)``
                # returned and a fresh loop was started). Both
                # signals are slot-killing; classify as disconnect
                # so the pool drops the bound-but-unusable connection.
                # Anchor to the canonical raise-site qualifier
                # ``"is bound to a"`` so user-supplied InterfaceError
                # messages mentioning event loops in a non-disconnect
                # context (e.g., "Cannot reuse a different event loop
                # topology in this driver", or an OTel trace message
                # mentioning "different event loop") do NOT false-
                # positive into disconnect classification. The
                # qualifier follows the canonical phrase verbatim at
                # dqliteclient/connection.py:2207, 2212 without
                # coupling to the ``DqliteConnection`` class name
                # (so a future class rename keeps the classification
                # correct).
                if (
                    "is bound to a closed event loop" in message
                    or "is bound to a different event loop" in message
                ):
                    return True
                # The dbapi raises ``InterfaceError("Connection
                # invalidated (id=...); reconnect before retrying
                # commit / rollback. ...")`` with ``code=None`` from
                # its pre-lock guard when ``_protocol`` is None
                # (i.e., a sibling task has invalidated the inner
                # transport). Match the full ``connection invalidated
                # (id=`` lexeme — the parenthesis is the dbapi's
                # contract at every raise site that surfaces the
                # cancel-after-invalidate signal: the four pre-lock /
                # in-lock guards in ``dqlitedbapi.aio.connection``
                # ``commit`` / ``rollback`` plus the two pre-lock
                # guards in ``dqlitedbapi.connection`` ``commit`` /
                # ``rollback`` (six total). The recognition + false-
                # positive guard live in
                # ``test_is_disconnect_recognises_connection_invalidated_interfaceerror.py``;
                # so a user-raised ``InterfaceError("Connection
                # invalidated by trigger BEFORE INSERT")`` (no
                # ``(id=``) does NOT trip disconnect classification.
                if "connection invalidated (id=" in message:
                    return True
                # The dbapi raises ``InterfaceError("Connection used
                # after fork; reconstruct from configuration in the
                # target process. ...")`` (and the aio twin
                # ``AsyncConnection used after fork; ...``) from
                # every method when the cached creator-pid no longer
                # matches the current process. The fork-inherited
                # slot is permanently dead — the parent's asyncio
                # loop / asyncio.Lock state is unusable in the child.
                # Match the canonical phrase ``"used after fork"`` so
                # SA's pool invalidates and the next acquire re-builds.
                # Fork-based deployments (gunicorn preload mode,
                # Celery prefork, multiprocessing.Pool) inheriting
                # engine slots from the parent rely on this
                # classifier to self-heal. The substring is
                # consistent across all raise sites in
                # ``dqlitedbapi.connection`` and
                # ``dqlitedbapi.aio.connection`` and is unlikely to
                # collide with user-raised wording (the bare token
                # "fork" would, "used after fork" does not).
                if "used after fork" in message:
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
                # Go-parity ``errNotFound → ErrBadConn`` arm:
                # SQLITE_NOTFOUND (=12) is overloaded between the
                # leader-flip ``gateway.c::LOOKUP_DB`` arm (message
                # "no database opened ...") and the orthogonal
                # ``LOOKUP_STMT`` arm (server-side state bug, kept
                # as a non-disconnect InternalError). Substring-gate
                # on the wire-side ``LEADER_LOST_DB_LOOKUP_SUBSTRING``
                # so the leader-flip arm participates in pool
                # invalidation without over-triggering on the stmt-id
                # path. Mirrors the parallel arm in
                # ``dqliteclient/connection.py::_run_protocol``.
                if (
                    isinstance(cause, err_class)
                    and getattr(cause, "code", None) == _SQLITE_NOTFOUND
                ):
                    # ``msg_lc`` actually lower-case (matching the
                    # variable name) — defends against a future
                    # upstream wording capitalisation change that
                    # would silently break the leader-flip classifier
                    # arm. ``LEADER_LOST_DB_LOOKUP_SUBSTRING`` is
                    # already lowercase by convention (the wire
                    # layer's SSOT pins it).
                    raw = (
                        getattr(cause, "raw_message", None) or getattr(cause, "message", None) or ""
                    )
                    msg_lc = raw.lower()
                    if msg_lc.startswith(LEADER_LOST_DB_LOOKUP_SUBSTRING):
                        return True
            # Substring scan — restricted to OperationalError(code=None)
            # (the wire-decode / ProtocolError / cross-loop-remap
            # surface) and bare DatabaseError with codes 11/24/26
            # (CORRUPT / FORMAT / NOTADB). Server-routed coded
            # OperationalErrors carry user-controlled message text and
            # must NOT trip disconnect classification on a benign
            # RAISE that happens to contain a transport-style
            # substring. Use ``raw_message`` first so a >4096-char
            # server message whose disconnect substring sits past the
            # dbapi's ``_DEFAULT_MAX_RAW_MESSAGE`` truncation boundary
            # is still classified — the raw_message slot carries the
            # full server text up to the wire-layer FailureResponse
            # limit (~64 KiB).
            if isinstance(cause, _dbapi_exc.OperationalError):
                cause_code = getattr(cause, "code", None)
                applies_substring = cause_code is None
                # Narrow second channel for the ``translateRaftErrCode``
                # default-arm collapse: cluster-mgmt RAFT errors
                # (SHUTDOWN / CANCELED / NOCONNECTION / etc.) reach us
                # as ``OperationalError(code=1)`` with verbatim
                # ``raft_strerror`` text. The standard ``code is None``
                # gate keeps these from the general substring scan;
                # match against a tightly-bounded marker set so the
                # cluster-state signal still classifies as disconnect
                # without re-opening the broader substring scan to
                # server-controlled message text. See
                # ``_RAFT_COLLAPSE_DISCONNECT_MARKERS`` for the source
                # of truth.
                if not applies_substring and cause_code == _SQLITE_ERROR_CODE:
                    text = getattr(cause, "raw_message", None) or str(cause)
                    msg_lower = text.lower()
                    for marker in _RAFT_COLLAPSE_DISCONNECT_MARKERS:
                        if marker in msg_lower:
                            return True
            elif type(cause) is _dbapi_exc.DatabaseError:
                # Type-identity check (NOT isinstance) is deliberate.
                # ``DatabaseError`` has subclasses
                # (``OperationalError``, ``IntegrityError``,
                # ``InternalError``, ``ProgrammingError``,
                # ``DataError``, ``NotSupportedError``) which would
                # otherwise reach this branch via MRO. The intent of
                # this arm is the BARE ``DatabaseError`` instance
                # produced by ``_classify_operational`` for codes
                # 11/24/26 (CORRUPT/FORMAT/NOTADB) — see
                # ``_BARE_DBE_DISCONNECT_CODES``. Caller-side bug
                # classes (``ProgrammingError(code=SQLITE_MISUSE)``,
                # ``DataError``, etc.) MUST propagate as caller errors
                # rather than triggering pool invalidation: SA's retry
                # path would re-run the same broken caller code
                # against a fresh connection, duplicating non-
                # idempotent INSERTs. Today the typical caller-bug
                # codes (21 SQLITE_MISUSE, 25 SQLITE_RANGE) are absent
                # from ``_BARE_DBE_DISCONNECT_CODES`` so the
                # ``isinstance`` shape would be inert in practice, but
                # a future server release that adds CORRUPT/FORMAT/
                # NOTADB as an extended code on a subclass would
                # silently activate disconnect classification on
                # caller bugs. ``type is`` makes the scope explicit
                # and matches the docstring intent above.
                if getattr(cause, "code", None) in _BARE_DBE_DISCONNECT_CODES:
                    # Short-circuit ``return True`` mirrors ``do_ping``'s
                    # classification at base.py:2935-2945. A bare
                    # DatabaseError under one of the slot-fatal codes
                    # IS slot-fatal regardless of the message text —
                    # the substring scan exists for the
                    # ``OperationalError`` arm above where ``code`` is
                    # None and the message IS the load-bearing
                    # classification signal. The canonical engine
                    # wordings for CORRUPT / FORMAT / NOTADB
                    # ("database disk image is malformed", "file is
                    # not a database", "file is encrypted or is not a
                    # database") are NOT in ``_dqlite_disconnect_
                    # messages`` by design — those messages are caller
                    # diagnostics, not transport-state signals. Without
                    # this short-circuit, ``is_disconnect`` returned
                    # False on a real CORRUPT response, SA's pool
                    # returned the slot to the queue, and every
                    # subsequent checkout re-tripped the same fault.
                    # ``_BARE_DBE_DISCONNECT_CODES`` is the SSOT both
                    # classifiers gate on.
                    return True
                applies_substring = False
            elif isinstance(cause, _dbapi_exc.InterfaceError):
                # The dedicated-phrase arm at the top of this loop
                # (lines 2461-2539) already handles the
                # client-emitted InterfaceError shapes (closed
                # handle, event-loop binding faults, "connection
                # invalidated (id=", "used after fork") and
                # returns True directly on match. We only reach
                # here if none of those phrases matched, i.e. the
                # cause is most likely a server-emitted
                # InterfaceError carrying a transport-style
                # message under one of the disconnect-eligible
                # codes (``DQLITE_PROTO`` = 1001). The substring
                # scan below classifies on
                # ``_dqlite_disconnect_messages`` patterns that
                # are DISJOINT from the dedicated-phrase set, so
                # this is not a double scan — it is the second
                # half of a two-pass classifier. ``SQLITE_RANGE``
                # (25) / ``SQLITE_MISUSE`` (21) etc. are caller-
                # side bugs and MUST NOT trigger pool invalidation,
                # so restrict the substring scan to the explicit
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

    def do_recover_twophase(self, connection: Any) -> NoReturn:
        # ``NoReturn`` over ``list[Any]`` mirrors SA's ``raise``-only
        # stub convention (the dialect already uses ``NoReturn`` for
        # ``aio.py``'s ``callproc`` / ``nextset`` / ``scroll`` stubs)
        # so mypy / pyright correctly flag any code following a call
        # site as unreachable. The body unconditionally raises and
        # never returns a list; the prior annotation misled IDE
        # tooltips and type-checkers into expecting a successful
        # response.
        raise _dbapi_exc.NotSupportedError("dqlite does not support two-phase commit.")

    def do_close(self, dbapi_connection: Any) -> None:
        """SA pool checkin / ``engine.dispose()`` graceful path.

        Calls the dbapi's ``Connection.close()`` directly — no kwargs.
        The dbapi's ``close()`` runs the loop-thread shutdown bounded
        by the connection's ``_timeout`` (the operation lock acquire)
        plus a 5 s hard-coded thread-join window. The
        ``_close_timeout`` URL knob applies only to the fallback
        ``force_close_transport()`` path on this leg.

        On a transport-class close failure (``OperationalError``,
        ``InterfaceError``, ``DqliteConnectionError``, ``OSError`` —
        which covers ``TimeoutError`` / ``ConnectionResetError``) or
        on the documented cross-loop / dead-proxy raises from the
        dbapi's own close() machinery (``RuntimeError("Event loop is
        closed")`` / ``ReferenceError`` — both reachable on
        ``engine.dispose()``), fall back to
        ``force_close_transport()`` so the slot still releases — the
        graceful path tried, the operator gets a DEBUG log line, and
        the pool stays drainable. Programmer bugs (``AttributeError``,
        ``TypeError`` from a refactor) propagate through the
        narrowed except so they are not silently swallowed into the
        fallback.

        Happy-path mirrors ``aio.py``'s
        ``AsyncAdaptedConnection.close`` — both invoke the dbapi's
        rich (async-native on the aio side) close machinery first.
        The transport-class fallback leg, however, INTENTIONALLY
        reaches ``_force_close_transport`` (sync teardown) on BOTH
        the sync and async dialects: by the time the fallback fires,
        the async close has already been tried and failed; routing
        back through async close machinery just to fail again is
        over-engineering. ``aio.py``'s ``force_close_transport``
        public alias is the documented sync-teardown surface for
        exactly this leg.

        Both the first-close ``except`` and the fallback's
        ``contextlib.suppress`` use ``_FORCE_CLOSE_TAIL_EXCEPTIONS``
        (the wider tuple defined at module scope). An earlier shape
        used the narrower ``_TRANSPORT_CLASS_EXCEPTIONS`` on the
        first arm — but that narrower tuple omitted ``RuntimeError``
        (cross-loop ``RuntimeError("Event loop is closed")`` from
        the dbapi's writer-close machinery on a defunct loop) and
        ``ReferenceError`` (dead-proxy weakref on a half-collected
        ``AsyncAdaptedConnection``), both of which the wider tuple's
        docstring documents as reachable on ``engine.dispose()``
        paths. With the wider tuple on the first arm too, a
        ``RuntimeError("Event loop is closed")`` raised by the first
        ``close()`` itself (not just by ``force_close_transport``)
        is routed through the same fallback rather than escaping
        ``do_close`` and aborting SA's pool finalize.
        """
        try:
            dbapi_connection.close()
        except _FORCE_CLOSE_TAIL_EXCEPTIONS:
            # Use the wider tail tuple here (same as the fallback's
            # ``contextlib.suppress``) so a first-close raise of
            # ``RuntimeError("Event loop is closed")`` or
            # ``ReferenceError`` (both documented at
            # ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` as reachable from the
            # dbapi's own close() machinery during ``engine.dispose()``)
            # lands on the fallback rather than escaping
            # ``do_close``. The narrower first-arm tuple used to be
            # ``_TRANSPORT_CLASS_EXCEPTIONS``; that left the
            # "do_close never raises" invariant honoured only on the
            # second-close path. Programmer-bug classes
            # (``AttributeError`` / ``TypeError``) remain outside the
            # tuple so a refactor regression still surfaces.
            logger.debug(
                "do_close: graceful close raised transport-class error; "
                "falling back to force_close_transport",
                exc_info=True,
            )
            with contextlib.suppress(*_FORCE_CLOSE_TAIL_EXCEPTIONS):
                dbapi_connection.force_close_transport()

    def do_terminate(self, dbapi_connection: Any) -> None:
        """Force-close the connection without awaiting in-flight ops.

        SA's pool calls this for forced reclaim during
        ``engine.dispose()`` under failure or shutdown. ``has_terminate
        = True`` (above) promises SA that the path is bounded — unlike
        :meth:`do_close`, which routes through the dbapi's sync
        ``Connection.close()`` (which itself drives the bounded
        ``_run_sync(_close_async())`` shutdown on the loop thread,
        bounded by the dbapi connection's ``timeout`` attribute —
        ``dbapi_connection._timeout``, default 10 s, gated on a parked
        wire read). The dialect itself owns no ``_timeout`` attribute;
        the timeout lives on the dbapi connection and is configured
        via the ``timeout`` URL parameter / ``connect_args``.

        Routes through the dbapi's :meth:`Connection.force_close_transport`,
        which schedules ``writer.close()`` on the loop thread and
        bounds the thread join with ``close_timeout`` (default 0.5 s).
        Mirrors the async sibling at ``aio.py``.

        ``has_terminate=True`` promises a non-raising path; suppress
        any tail ``Exception`` so SA's pool finalize cannot crash on a
        partial-state connection (matches the async sibling's
        suppression discipline at ``aio.py``). ``KeyboardInterrupt``
        and ``SystemExit`` (and any other ``BaseException``) are
        deliberately NOT caught — they signal cooperative interpreter
        shutdown and a forced finalize must not mask them, mirroring
        the async sibling's stance on ``CancelledError``.

        Two-tier catch:

        * ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` — expected transport-class
          shapes (``OSError``, dbapi ``Error`` subclasses,
          ``DqliteConnectionError``, ``RuntimeError``,
          ``ReferenceError``). DEBUG-log + absorb.
        * Any other ``Exception`` — most likely a cross-repo dbapi
          refactor regression (``AttributeError`` from a rename,
          ``TypeError`` from a signature change, ``NotImplementedError``
          from a property swap). WARNING-log + absorb so SA's binary
          non-raising contract holds while the regression stays loudly
          observable in operator logs.
        """
        peer = _log_safe_peer(dbapi_connection)
        try:
            dbapi_connection.force_close_transport()
        except _FORCE_CLOSE_TAIL_EXCEPTIONS:
            # Expected shapes — DEBUG-log + absorb. The narrow tuple
            # covers transport-class failures (OSError + dbapi.Error
            # subclasses + DqliteConnectionError + RuntimeError +
            # ReferenceError) that ``force_close_transport`` legitimately
            # surfaces during forced disposal.
            logger.debug(
                "do_terminate: force_close_transport raised on dispose for "
                "peer=%s id=%s; proceeding (has_terminate=True non-raising "
                "contract)",
                peer,
                id(dbapi_connection),
                exc_info=True,
            )
        except Exception:
            # Unexpected shape — most likely a cross-repo dbapi
            # refactor regression (``force_close_transport`` renamed,
            # removed, signature-changed, or swapped to a raising
            # property). SA's ``has_terminate=True`` contract is
            # binary non-raising; absorbing here prevents the regression
            # from aborting ``engine.dispose()`` and leaking sibling
            # slots. WARNING-tier (vs DEBUG) so the regression stays
            # loudly visible in operator logs.
            logger.warning(
                "do_terminate: force_close_transport raised UNEXPECTED "
                "exception type on dispose for peer=%s id=%s; SA "
                "has_terminate=True contract absorbs to prevent dispose "
                "abort. Likely a dbapi-side refactor regression — "
                "investigate.",
                peer,
                id(dbapi_connection),
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
                cursor.execute(self._dialect_specific_select_one)
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
                #     AND server-emitted code-bearing variants. The
                #     project's ``_SERVER_INTERFACEERROR_DISCONNECT_CODES``
                #     constant explicitly EXCLUDES ``SQLITE_MISUSE`` (21)
                #     and ``SQLITE_RANGE`` (25) — those signal client-side
                #     programming errors (parameter index out of range,
                #     statement reused after finalize) and should NOT
                #     mark the slot unusable. The set is currently just
                #     ``{1001}`` (``DQLITE_PROTO``); see the constant's
                #     declaration block for the full exclusion rationale.
                #     For the pre-ping context, ANY ``InterfaceError``
                #     from ``SELECT 1`` indicates the slot is unusable —
                #     more conservative than ``is_disconnect``'s
                #     real-query classification, which restricts
                #     code-bearing ``InterfaceError`` to the narrow
                #     ``_SERVER_INTERFACEERROR_DISCONNECT_CODES`` set so
                #     caller-side bind misuse propagates as a real error.
                #     Asymmetry deliberate: pre-ping must reject any
                #     unusable slot; real-query must let caller-side
                #     errors surface.
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


# DDL kwarg-prefix runtime guard.
#
# SA's ``DialectKWArgs`` keys per-construct dialect kwargs by the
# user-written prefix; the inherited ``SQLiteDDLCompiler`` reads
# exclusively from ``dialect_options['sqlite']``. A
# ``Table(..., dqlite_with_rowid=False)`` therefore stored its value
# under ``dialect_options['dqlite']`` and was silently dropped at
# compile time. The dialect docstring warned about this in prose;
# the listener below enforces the discipline at construction time so
# a copy-paste mistake from a pysqlite-tagged-with-dialect-name
# example surfaces a sharp ``ArgumentError`` rather than a silently
# dropped kwarg. Mirrors the connect-side
# ``_validate_connect_kwargs`` discipline.
def _dqlite_prefix_ddl_guard(target: Any, parent: Any) -> None:
    dqlite_opts = target.dialect_options.get("dqlite")
    if dqlite_opts is None:
        return
    # ``_non_defaults`` is SA's source of truth for "user actually
    # passed this kwarg" — the public ``dict``-view always includes
    # the registry defaults. Reading the private attribute keeps the
    # guard tight (no false positives on inherited defaults).
    non_defaults = getattr(dqlite_opts, "_non_defaults", None)
    if not non_defaults:
        return
    offenders = sorted(non_defaults.keys())
    hint = ", ".join(f"sqlite_{key}" for key in offenders)
    raise ArgumentError(
        f"{type(target).__name__} received dqlite_* DDL kwarg(s) "
        f"{sorted(f'dqlite_{k}' for k in offenders)}: the inherited "
        f"SQLiteDDLCompiler reads exclusively from "
        f"dialect_options['sqlite'], so the dqlite-prefixed value(s) "
        f"would be silently dropped at compile time. Did you mean: "
        f"{hint}? Mirrors the connect-side _validate_connect_kwargs "
        f"fail-fast discipline; the DDL-side guard refuses to let a "
        f"copy-paste mistake silently disable a constraint, where "
        f"clause, or rowid option."
    )


def _install_dqlite_prefix_ddl_guard() -> None:
    """Register the prefix guard against the DDL constructs SA's
    SQLite dialect declares ``construct_arguments`` for. Idempotent
    so a re-import (test runner) does not double-attach the listener.
    """
    from sqlalchemy import event
    from sqlalchemy.sql import schema as sa_schema

    _ATTR = "_dqlite_prefix_guard_installed"
    if getattr(_install_dqlite_prefix_ddl_guard, _ATTR, False):
        return
    for cls in (sa_schema.Table, sa_schema.Index, sa_schema.Column, sa_schema.Constraint):
        event.listen(cls, "after_parent_attach", _dqlite_prefix_ddl_guard)
    setattr(_install_dqlite_prefix_ddl_guard, _ATTR, True)


_install_dqlite_prefix_ddl_guard()
