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
from sqlalchemy.engine import characteristics as _sa_characteristics
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
    """URL-time timeout validator delegating to the client-layer ``validate_timeout`` SSOT.

    Returns ``True`` on success; translates client ``ValueError`` / ``TypeError``
    to ``ArgumentError`` so URL-parse errors surface as ``ArgumentError``.
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


# Server-side InterfaceError codes whose transport-style message the substring
# scanner may classify as a disconnect. SQLITE_RANGE (25) / SQLITE_MISUSE (21)
# are excluded: caller-side bind bugs must not retry against a fresh connection.
_SERVER_INTERFACEERROR_DISCONNECT_CODES: Final[frozenset[int]] = frozenset({DQLITE_PROTO})

# Primary SQLite codes routing to bare ``DatabaseError`` that the dialect treats
# as slot-fatal in both ``is_disconnect`` and ``do_ping``. Today: CORRUPT / FORMAT
# / NOTADB — unambiguous regardless of message and the only codes the server emits
# as bare DatabaseError.
_BARE_DBE_DISCONNECT_CODES: Final[frozenset[int]] = BARE_DATABASE_ERROR_CODES

# URL-time defense-in-depth cap on ``max_continuation_frames``: 10× the wire-layer
# default, so a typo like ``?max_continuation_frames=9999999999999999`` can't
# silently collapse the ceiling. Tied to the wire constant via import.
_URL_MAX_CONTINUATION_FRAMES_FACTOR: Final[int] = 10
_URL_MAX_CONTINUATION_FRAMES_CAP: Final[int] = (
    _URL_MAX_CONTINUATION_FRAMES_FACTOR * DEFAULT_MAX_CONTINUATION_FRAMES
)

# RAFT-collapse marker phrases for the narrow ``code=1`` substring scan: the C
# ``translateRaftErrCode`` collapses RAFT_SHUTDOWN / CANCELED / NOCONNECTION to
# SQLITE_ERROR (=1) with verbatim ``raft_strerror`` text. Whole canonical phrases
# only (not single words) to avoid false-positives on user RAISE() messages.
_RAFT_COLLAPSE_DISCONNECT_MARKERS: Final[tuple[str, ...]] = (
    "server is shutting down",
    "operation canceled",
    "no connection to remote server",
)
# ``translateRaftErrCode`` default-arm code; not exposed by ``dqlitewire.constants``
# (generic SQLite primary, not a dqlite extended code).
_SQLITE_ERROR_CODE: Final[int] = 1

# Transport-class exceptions for best-effort cleanup paths that must swallow a
# flaky close / rollback without aborting ``engine.dispose()``. Narrow on purpose:
# programmer-bug shapes (AttributeError, TypeError, bare RuntimeError) propagate.
_TRANSPORT_CLASS_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    _dbapi_exc.OperationalError,
    _dbapi_exc.InterfaceError,
    _client_exc.DqliteConnectionError,
    OSError,
)

# Wider suppression set used on both arms of ``do_close`` (and ``do_terminate``)
# to keep the "never raises" invariant. Two extra classes vs. the transport set:
# ``RuntimeError("Event loop is closed")`` from the dbapi writer-close on a defunct
# loop, and ``ReferenceError`` from a half-collected ``AsyncAdaptedConnection``
# weakproxy. ``AttributeError`` / ``TypeError`` still propagate.
_FORCE_CLOSE_TAIL_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    *_TRANSPORT_CLASS_EXCEPTIONS,
    RuntimeError,
    ReferenceError,
)

logger = logging.getLogger(__name__)


# Cap server-controlled TEXT cells before they enter %r-formatted log lines:
# oversized TEXT (wire cap 64 MiB) per result row is a log-infra DoS vector.
_LOG_TRUNCATE_MAX_CHARS: Final[int] = 200


def _truncate_for_log(value: str) -> str:
    """Truncate ``value`` to ``_LOG_TRUNCATE_MAX_CHARS`` with a dropped-chars marker.

    Truncation only — does NOT sanitize; use :func:`_safe_for_log` for
    server-controlled input.
    """
    if len(value) <= _LOG_TRUNCATE_MAX_CHARS:
        return value
    overflow = len(value) - _LOG_TRUNCATE_MAX_CHARS
    return f"{value[:_LOG_TRUNCATE_MAX_CHARS]}... [truncated, {overflow} chars]"


def _safe_for_log(value: str) -> str:
    """Sanitize control / bidi / invisible chars AND truncate for log embedding.

    Closes the journald U+2028 record-separator log-injection gap that ``%r``
    leaves open; use at any site interpolating server-controlled text.
    """
    return _truncate_for_log(_sanitize_server_text(value))


def _log_safe_peer(obj: object) -> str | None:
    """Return ``obj.address`` sanitized for line-oriented log output, or ``None``.

    Uses the wire-layer ``sanitize_for_log`` (escapes LF/tab so a hostile peer
    can't inject fake log lines). Load-bearing for ``dial_func`` overrides and
    post-redirect ``_address`` updates that bypass the ``parse_address`` gate.
    """
    addr = getattr(obj, "address", None)
    if addr is None:
        return None
    return _sanitize_for_log(str(addr))


__all__ = ["DqliteCompiler", "DqliteDialect"]

_TRUE_TOKENS: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS: Final[frozenset[str]] = frozenset({"0", "false", "no", "off"})

# SSOT for the AUTOCOMMIT-rejection diagnostic shared across the four reject-sites.
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
    """Yield ``e`` and each ``__cause__`` / ``__context__`` / ``BaseExceptionGroup``
    child via BFS, bounded by ``max_depth`` / ``max_nodes`` / a visited set.

    Multi-hop walk handles wrap towers (retry/telemetry/circuit-breaker) and the
    pool's aggregated connect-failure groups that a single-hop check would miss.
    Group children enqueue at the parent's depth (fan-out, not a wrap layer).
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
        for nxt in (cur.__cause__, cur.__context__):
            if nxt is not None:
                queue.append((nxt, depth + 1))
        if isinstance(cur, BaseExceptionGroup):
            for child in cur.exceptions:
                queue.append((child, depth))


def _parse_url_int_or_none(key: str, raw: str, *, upper: int) -> int | None:
    """Strict parser for URL row/frame governors: ``"none"`` -> ``None`` (disables
    the cap), an int in ``1..upper`` -> the int, else ``ArgumentError``.
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
    """Strict bool parser for URL query params; ``ArgumentError`` on unknown tokens."""
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

    Parses str cells via ``datetime.fromisoformat`` (dqlitedbapi has no
    detect_types auto-decode); logs+passes through unparseable TEXT rather than
    aborting the read. Mixed-writer hazard: under ``timezone=False`` a non-UTC
    naive cell from a native peer is wrong by the writer's offset — use
    ``timezone=True`` or enforce UTC writes app-side.
    """

    # Pysqlite-only kwargs the dqlite processors do NOT consult; reject so the
    # divergence surfaces here, not as a bare TypeError far from the Column().
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
            # Reject time-only payload (bind-side mirror of the result-side raise),
            # else the round-trip writes a cell the same reader rejects.
            if isinstance(value, datetime.time) and not isinstance(value, datetime.datetime):
                raise _dbapi_exc.DataError(
                    f"DateTime column cannot bind time-only payload "
                    f"{value!r}: no defensible date to fabricate."
                )
            # Widen bare ``date`` to midnight ``datetime`` and emit the formatted
            # string directly (always-on six-digit microseconds) for byte-identical
            # pysqlite parity — its ``_storage_format`` always includes the suffix
            # while dqlitedbapi's encoder omits it when microsecond == 0.
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
                # Reuse the dbapi offset formatter so suffix rendering stays in
                # lockstep with the wire codec.
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
        """Render an inline ``DateTime`` literal with always-on six fractional
        digits for byte-for-byte pysqlite parity (the inherited renderer omits
        the suffix when microsecond == 0).
        """
        bind = self.bind_processor(dialect)
        assert bind is not None

        def process(value: Any) -> str:
            if value is None:
                return "NULL"
            return f"'{bind(value)}'"

        return process

    # One-shot per-class WARNING gate so processor churn does not re-arm it.
    _unparseable_iso_warning_emitted: ClassVar[bool] = False

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        want_timezone = self.timezone

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, str):
                # Affinity-stripped (TEXT-tagged) cell: dqlitedbapi ran no converter.
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
                    elif logger.isEnabledFor(logging.DEBUG):
                        # Gate on level so the per-row sanitiser walk is skipped
                        # when DEBUG is filtered (1M-row scans are expensive).
                        logger.debug(
                            "DateTime processor received unparseable ISO8601 string %r: %s",
                            _safe_for_log(value),
                            _safe_for_log(str(e)),
                        )
                    return value
            if isinstance(value, datetime.datetime):
                if want_timezone:
                    # timezone=True promises aware; attach UTC to a naive cell.
                    if value.tzinfo is None:
                        return value.replace(tzinfo=datetime.UTC)
                    return value
                # timezone=False: convert through UTC first so a non-UTC aware
                # input's actual instant is preserved, not just the wall clock.
                if value.tzinfo is not None:
                    return value.astimezone(datetime.UTC).replace(tzinfo=None)
                return value
            if isinstance(value, datetime.time):
                # Time-only payload in a DateTime column: no defensible date to
                # fabricate (unlike the Time-receives-datetime narrowing). Raise.
                raise _dbapi_exc.DataError(
                    f"DateTime column received time-only payload "
                    f"{value!r}: the cell decodes as datetime.time and "
                    f"there is no defensible date to fabricate."
                )
            return value

        return process


class _DqliteDate(sqltypes.Date):
    """Date processor handling datetime and ISO8601-string inputs.

    Narrows ``datetime.datetime`` to ``datetime.date`` (tzinfo dropped, so the
    result is the UTC day, not the local day) and parses str cells via
    ``date.fromisoformat``; logs+passes through unparseable TEXT.
    """

    # Pysqlite-only kwargs the dqlite processors do NOT consult.
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
            # Reject time-only payload (bind-side mirror of the result-side raise).
            if isinstance(value, datetime.time) and not isinstance(value, datetime.datetime):
                raise _dbapi_exc.DataError(
                    f"Date column cannot bind time-only payload "
                    f"{value!r}: a time has no date component."
                )
            # Narrow to ``date`` (tzinfo dropped) so the wire format matches
            # pysqlite's ``"YYYY-MM-DD"`` rather than a full timestamp.
            if isinstance(value, datetime.datetime):
                return value.date()
            return value

        return process

    _unparseable_iso_warning_emitted: ClassVar[bool] = False

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                return value.date()  # tzinfo dropped; see class docstring
            if isinstance(value, datetime.time):
                # Time-only payload in a Date column: no defensible date. Raise.
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
                    elif logger.isEnabledFor(logging.DEBUG):
                        # Level-gated: skip the per-row sanitiser walk when filtered.
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

    dqlitedbapi already decodes time payloads to ``datetime.time``, so the
    inherited pysqlite ``TIME`` processor would call ``fromisoformat`` on one and
    raise ``TypeError``. Passes ``time`` through, parses str via
    ``time.fromisoformat``, logs+passes unparseable TEXT. ``bind_processor``
    raises ``DataError`` on cross-type payloads and emits six-digit microseconds.
    """

    # Pysqlite-only kwargs the dqlite processors do NOT consult.
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
            # Reject cross-type datetime/date payloads at bind. ``datetime``
            # ordered before ``date`` because the former is a subclass.
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
            # Emit six-digit microseconds directly for pysqlite parity (the wire
            # encoder omits the suffix when microsecond == 0).
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
        """Render an inline ``Time`` literal with always-on six fractional digits
        for byte-for-byte pysqlite parity (the inherited renderer omits the suffix
        when microsecond == 0); tz-aware values use the wire-codec offset suffix.
        """

        def process(value: Any) -> str:
            if value is None:
                return "NULL"
            if not isinstance(value, datetime.time):
                return f"'{value!s}'"
            base = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"
            if value.tzinfo is None:
                return f"'{base}'"
            # dbapi offset formatter keeps suffix rendering in lockstep with the wire codec.
            offset = value.utcoffset()
            if offset is None:
                return f"'{base}'"
            from dqlitedbapi.types import _format_utc_offset

            return f"'{base + _format_utc_offset(offset)}'"

        return process

    _unparseable_iso_warning_emitted: ClassVar[bool] = False

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        want_timezone = self.timezone

        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                # Datetime payload in a Time column: narrow via ``.timetz()``
                # (not ``.time()``) so the source offset survives into the next
                # branch — else a naive-from-aware value would get UTC re-attached,
                # silently shifting the instant. ``datetime`` is not a ``time``
                # subclass, so both branches need explicit handling.
                value = value.timetz()
            if isinstance(value, datetime.time):
                if want_timezone:
                    # timezone=True promises aware; attach UTC to a naive cell.
                    if value.tzinfo is None:
                        return value.replace(tzinfo=datetime.UTC)
                    return value
                # timezone=False: strip tzinfo. ``time`` has no ``astimezone``
                # (no date for DST), so a fixed-offset convert is unsafe; pysqlite
                # also drops tzinfo.
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
                    elif logger.isEnabledFor(logging.DEBUG):
                        # Level-gated: skip the per-row sanitiser walk when filtered.
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

    dqlite has no UDF primitive, so SQLite's REGEXP operator (which pysqlite
    backs via ``create_function``) is unsupported. Raise ``NotSupportedError`` at
    compile time on both regexp visitors (the negated form is a separate SA
    dispatcher, not auto-derived) instead of a late "no such function" from the
    cluster after a SAVEPOINT round-trip.
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

    # ``floor`` is a pysqlite UDF papering over SQLite built without
    # SQLITE_ENABLE_MATH_FUNCTIONS; dqlite has no UDF primitive. Raise at compile
    # time (symmetric with the regexp visitors). Operators with math functions
    # enabled can subclass and override ``visit_function`` to relax this.
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


class DqliteSessionModeCharacteristic(_sa_characteristics.ConnectionCharacteristic):
    """Per-connection ``dqlite_session_mode`` characteristic.

    Stores the mode on the underlying dqlitedbapi connection (unwrapped via
    ``_unwrap_dqlite_connection``) so ``do_begin`` and the cursor BEGIN-rewrite
    see it. ``transactional = True`` so mid-transaction toggles raise. Emits
    ``PRAGMA query_only = N`` only when crossing the read_only boundary, before
    updating the live attribute; force-closes the slot if the PRAGMA raises.
    """

    transactional: ClassVar[bool] = True

    def get_characteristic(self, dialect: Any, dbapi_conn: Any) -> str:
        target = dialect._unwrap_dqlite_connection(dbapi_conn)
        return getattr(target, "_dqlite_session_mode", "immediate")

    def set_characteristic(self, dialect: Any, dbapi_conn: Any, value: Any) -> None:
        target = dialect._unwrap_dqlite_connection(dbapi_conn)
        requested = dialect._validate_dqlite_session_mode(value)
        current = getattr(target, "_dqlite_session_mode", "immediate")
        if current == requested:
            return
        need_query_only = 1 if requested == "read_only" else 0
        was_query_only = 1 if current == "read_only" else 0
        try:
            if need_query_only != was_query_only:
                # Emit on the SA-side dbapi cursor (async routes through the
                # AsyncAdapt cursor under greenlet_spawn).
                cur = dbapi_conn.cursor()
                try:
                    cur.execute(f"PRAGMA query_only = {need_query_only}")
                finally:
                    cur.close()
        except BaseException:
            # Don't return a poisoned slot to the pool; suppress force-close's own
            # exceptions so the originating one propagates and SA invalidates.
            with contextlib.suppress(Exception):
                target.force_close_transport()
            raise
        target._dqlite_session_mode = requested

    def reset_characteristic(self, dialect: Any, dbapi_conn: Any) -> None:
        # SA's reset passes no "before" value; restore the construct-time default
        # captured once on ``_dqlite_session_mode_default``.
        target = dialect._unwrap_dqlite_connection(dbapi_conn)
        default = getattr(target, "_dqlite_session_mode_default", "immediate")
        self.set_characteristic(dialect, dbapi_conn, default)


class DqliteDialect(SQLiteDialect_pysqlite):
    """SQLAlchemy dialect for dqlite, inheriting ``SQLiteDialect_pysqlite``.

    Deliberate divergences from the parent (documented at each override site):
    ``import_dbapi`` returns ``dqlitedbapi``; ``create_connect_args`` parses the
    ``dqlite://host:port/db?...`` URL; AUTOCOMMIT is rejected; ``on_connect`` is a
    no-op (no UDF primitive); ``is_disconnect`` walks cause chains broadly.

    DDL kwargs use the ``sqlite_*`` prefix, NOT ``dqlite_*`` (the inherited
    ``SQLiteDDLCompiler`` reads only ``dialect_options['sqlite']``; the
    ``dqlite_*`` form is silently dropped — enforced by the DDL guard below).

    Only SERIALIZABLE is honoured. ``_isolation_lookup`` is the truthful set;
    ``get_isolation_level_values()`` additionally advertises AUTOCOMMIT as a
    diagnostic-routing channel so SA dispatches to our dedicated rejection message
    rather than its generic "unknown isolation level" error.
    """

    name = "dqlite"

    # One-shot WARNING gate for ``?max_total_rows=none`` (per dialect class).
    _max_total_rows_disabled_warning_emitted: ClassVar[bool] = False

    # Pin the default isolation level statically (test harnesses skipping
    # ``initialize()`` still see it). dqlite is single-leader Raft: always SERIALIZABLE.
    default_isolation_level = "SERIALIZABLE"

    # Register ``dqlite_session_mode`` alongside the inherited characteristics so
    # ``execution_options(dqlite_session_mode=…)`` works at every scope.
    connection_characteristics = util.immutabledict(
        {
            **SQLiteDialect_pysqlite.connection_characteristics,
            "dqlite_session_mode": DqliteSessionModeCharacteristic(),
        }
    )

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        # Pin QueuePool: dqlite is a remote dbapi (one socket per connection, no
        # thread-sharing); SingletonThreadPool would break threadsafety=1.
        return pool.QueuePool

    # dbapi-module-name convention (the sync URL has no ``+driver`` suffix to
    # mirror); renders ``dialect_description = "dqlite+dqlitedbapi"``. The async
    # sibling uses ``driver = "aio"`` for ``dqlite+aio://...`` URL parity.
    driver = "dqlitedbapi"

    # Drift-defence pin (inherited False) matching the async sibling's True.
    is_async = False

    # paramstyle (qmark) is inherited transitively via ``self.dbapi.paramstyle``;
    # no class-level override needed.

    # SA reads ``__class__.__dict__`` (not the inherited attr) for statement-cache
    # discovery, so inheriting would silently disable caching. Pin explicitly.
    supports_statement_cache = True

    # returns_native_bytes (True) inherited from pysqlite. dqlitedbapi returns
    # native ``bytes`` for BLOB columns, so the result processor skips the rewrap.

    # Drift-defence pin: dqlitedbapi returns ``str`` column names; a non-None value
    # would route descriptions through SA's byte-decode pipeline and crash.
    description_encoding = None

    # dqlitedbapi cursors are buffered (no SA server-side cursor protocol).
    # Inherited default False; pinned for drift defence.
    supports_server_side_cursors = False

    # No SEQUENCE primitive; pinned (inherited False) so a default flip can't emit
    # SEQUENCE DDL the cluster rejects.
    supports_sequences = False

    # INTEGER PRIMARY KEY AUTOINCREMENT, not SQL-standard IDENTITY; pinned (inherited
    # False) for drift defence.
    supports_identity_columns = False

    # Autoincrement rowids are populated post-INSERT (no pre-execute sequence);
    # pinned (inherited False) so a flip can't trigger spurious pre-execute SELECTs.
    preexecute_autoincrement_sequences = False

    # Pin True (inherited False) so SA's forced-reclaim lands on ``do_terminate``
    # (bounded by close_timeout ~0.5 s) rather than do_close's ~10 s wire-read wait,
    # which would block shutdown SLAs under partition + SIGTERM.
    has_terminate = True

    # dqlite has a first-class BOOLEAN wire tag, so no CHECK (col IN (0,1)) needed.
    # Round-trip caveat: a bool stored in an INTEGER column reads back as int 1/0
    # (``== True`` holds, ``is True`` does not) — declare ``Column(Boolean)`` for
    # round-trip identity. Mirrors stdlib sqlite3; the user owns the column type.
    supports_native_boolean = True
    # Inert today (SA gates the check-constraint behind supports_native_boolean);
    # pinned in lockstep in case SA decouples the two.
    non_native_boolean_check_constraint = False

    # No native DECIMAL (stored TEXT / REAL); matches pysqlite, pinned for drift.
    supports_native_decimal = False

    # SQLite >= 3.35 supports RETURNING on I/U/D; pin (all default True on parent)
    # so version-gated upstream discovery can't silently change behaviour.
    insert_returning = True
    update_returning = True
    delete_returning = True
    # update_returning_multifrom (True) inherited stably from the parent class body
    # (not its version gate, which resets only the trio above).

    # Executemany-RETURNING flags. dqlitedbapi's executemany accumulates per-param
    # RETURNING rows so all three DML kinds deliver in one call. INSERT is a memoized
    # DefaultDialect property; UPDATE/DELETE default False there. Pin all True.
    insert_executemany_returning = True
    update_executemany_returning = True
    delete_executemany_returning = True

    # Multi-row INSERT VALUES (insertmanyvalues depends on it); pinned for drift.
    supports_multivalues_insert = True

    # Rowcount truthfulness flags. SQLite reports accurate U/D and aggregated
    # executemany rowcounts; ``_returning`` is False (insertmanyvalues-with-RETURNING
    # uses separate accounting). Pinned (matches parent) for drift defence.
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_sane_rowcount_returning = False

    # Insert-path flags keyed by insertmanyvalues codegen / DEFAULT VALUES / rowid.
    # ``supports_default_values`` also re-pinned in __init__ (parent version-gates it).
    use_insertmanyvalues = True
    supports_default_values = True
    insert_null_pk_still_autoincrements = True
    # Remaining insertmanyvalues / bind_typing / for_update_of pins (DefaultDialect
    # defaults). ``..._sort_by_parameter_order`` is a memoized property derived from
    # ``insert_returning and use_insertmanyvalues``; pinned explicitly = same value.
    use_insertmanyvalues_wo_returning = False
    insertmanyvalues_implicit_sentinel = InsertmanyvaluesSentinelOpts.NOT_SUPPORTED
    supports_for_update_of = False
    insert_executemany_returning_sort_by_parameter_order = True
    bind_typing = BindTyping.NONE

    # Override pysqlite's date/time processors: pysqlite's expect stdlib sqlite3's
    # detect_types auto-decode, but dqlitedbapi delivers TEXT cells as plain strings
    # (or already-decoded datetimes). The inherited ``Time`` would raise TypeError on
    # a decoded ``datetime.time``. ``TIMESTAMP`` is mapped explicitly to guard against
    # an MRO change re-binding it to pysqlite's processor.
    colspecs = {
        **SQLiteDialect_pysqlite.colspecs,
        sqltypes.DateTime: _DqliteDateTime,
        sqltypes.Date: _DqliteDate,
        sqltypes.Time: _DqliteTime,
        sqltypes.TIMESTAMP: _DqliteDateTime,
    }

    # Compiler subclass that raises at compile time on regexp_match (no UDF
    # primitive); see ``DqliteCompiler``.
    statement_compiler = DqliteCompiler

    def __init__(self, **kwargs: Any) -> None:
        # Keyword-only signature: forwarding ``*args`` would bind a caller's
        # positional to whichever parent slot lines up. SA-internal construction
        # always uses kwargs, so this closes the positional foot-gun.
        #
        # Reject paramstyle != qmark up-front (the dbapi only accepts qmark; a
        # silent ``named`` would compile ``:name`` placeholders). ``None`` is the
        # SA "use dbapi default" sentinel and resolves to qmark.
        ps = kwargs.get("paramstyle")
        if ps is not None and ps != "qmark":
            raise ArgumentError(f"dqlite dialect requires paramstyle='qmark'; got {ps!r}")
        # Eager AUTOCOMMIT rejection so the error points at the kwarg instead of a
        # deferred connect-time pool traceback. SA normalises underscores, so only
        # the spaceless case-insensitive form reaches us.
        iso_level = kwargs.get("isolation_level")
        if isinstance(iso_level, str) and iso_level.upper() == "AUTOCOMMIT":
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        # Eager native_datetime rejection: the dqlite date/time processors don't
        # consult it (pysqlite's are different processors), so accepting it would
        # diverge from the documented pysqlite semantics.
        if "native_datetime" in kwargs:
            raise ArgumentError(
                "dqlite dialect does not honour ``native_datetime``: the "
                "dqlite-specific date/time processors do not consult this "
                "flag (pysqlite's are different processors). Pass dates as "
                "Python ``datetime`` / ``date`` objects directly; the "
                "wire-layer ISO8601 codec round-trips them losslessly."
            )
        super().__init__(**kwargs)
        # Re-apply the RETURNING / default-values / multivalues pins at instance
        # level: the parent's ``__init__`` version-gates them on
        # ``sqlite_version_info < (3,35) or util.pypy`` and the PyPy arm would
        # unconditionally zero RETURNING, shadowing the class-level pins above.
        self.insert_returning = True
        self.update_returning = True
        self.delete_returning = True
        self.supports_default_values = True
        self.supports_multivalues_insert = True

        # Same drift defence for the parent's version-gated
        # insertmanyvalues_max_parameters (DefaultDialect's value, SA 2.x: 32700).
        self.insertmanyvalues_max_parameters = 32700

    @classmethod
    def import_dbapi(cls) -> types.ModuleType:
        # Sync ``dqlitedbapi``; the async dialect overrides to ``dqlitedbapi.aio``.
        import dqlitedbapi

        return dqlitedbapi

    # Full set of dbapi.connect kwargs the dialect forwards. The URL-query path is
    # restricted to ``_URL_QUERY_ALLOWED`` (typed convert + range check); the
    # ``connect_args=`` path bypasses that, so ``connect()`` revalidates the merged
    # set against this allowlist to catch typos at first checkout with the same
    # ArgumentError the URL path emits. ``dial_func`` is connect_args-only (a URL
    # string can't carry a callable). Other entries: stdlib sqlite3 parity
    # (busy_timeout, check_same_thread), wire/timeout knobs, and session_mode
    # (engine-wide BEGIN-form / read-only default; per-session form is the
    # ``dqlite_session_mode`` execution-option).
    _CONNECT_KWARG_ALLOWED: frozenset[str] = frozenset(
        {
            "address",
            "database",
            "timeout",
            "max_total_rows",
            "max_continuation_frames",
            "max_message_size",
            "trust_server_heartbeat",
            "close_timeout",
            "dial_timeout",
            "attempt_timeout",
            "dial_func",
            "busy_timeout",
            "check_same_thread",
            "session_mode",
        }
    )

    # Per-key (converter, validator) tuples. URL path runs converter then validator;
    # connect_args path runs only the validator (values already typed) — so every
    # key MUST carry a non-None validator that fully describes the in-range shape.
    # The ``not isinstance(v, bool)`` guards reject ``True`` as int 1 on the
    # connect_args path (the URL path never carries a bool).
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
        # ``max_message_size``: wire inbound frame cap. ``None`` = wire-default
        # 64 MiB. Int range mirrors max_total_rows; the wire layer is the SSOT for
        # the real upper bound (a SA cap would mask legitimate larger values).
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
        # close_timeout floor 0.01 s (below it the loop has too few ticks to flush
        # FIN, leaving connections in TIME_WAIT); delegated to the client-layer
        # ``validate_timeout`` SSOT, which translates errors to ArgumentError.
        "close_timeout": (float, _validate_close_timeout_url),
        # go-dqlite parity knobs (Config.DialTimeout / AttemptTimeout), same
        # validate_timeout SSOT; neither gates FIN-flush so no close_timeout floor.
        "dial_timeout": (float, _validate_dial_timeout_url),
        "attempt_timeout": (float, _validate_attempt_timeout_url),
        # ``busy_timeout`` (seconds) — stdlib sqlite3 parity; non-negative (0 = no
        # retry), finite, non-bool (the floor lives here since connect_args bypasses
        # the converter).
        "busy_timeout": (
            float,
            lambda v: (
                not isinstance(v, bool)
                and isinstance(v, int | float)
                and math.isfinite(v)
                and v >= 0
            ),
        ),
        # ``check_same_thread`` (bool) — stdlib sqlite3 parity; strict-bool
        # validator (rejects int 0/1).
        "check_same_thread": (
            lambda s: _parse_url_bool("check_same_thread", s),
            lambda v: isinstance(v, bool),
        ),
        # ``session_mode`` (str) — URL ``?session_mode=read_only`` etc.; lowercased
        # to canonical form before storing.
        "session_mode": (
            lambda s: s.lower(),
            lambda v: (
                isinstance(v, str)
                and v.lower() in {"immediate", "deferred", "exclusive", "read_only"}
            ),
        ),
    }

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from a ``dqlite://host:port/database?...`` URL.

        Known query params are typed and range-validated at parse time so typos /
        unparseable / out-of-range values raise :class:`ArgumentError` before any
        pool is built. A repeated key takes its last occurrence (``raw[-1]``). The
        URL carries one host:port; the client discovers other nodes from it, so an
        unreachable URL host cannot bootstrap leader-discovery.
        """
        # Reject any userinfo (``is not None`` so even a bare ``@`` / empty
        # username is caught): dqlite has no auth; URL credentials would be dropped.
        if url.username is not None or url.password is not None:
            raise ArgumentError(
                "Invalid URL: dqlite does not accept username or password in the URL"
            )
        # SA's ``make_url`` does NOT split fragments, so a ``#`` lands in the query
        # value or database name. Detect and raise a clear "fragment" diagnostic.
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
            # SA's parser allows out-of-range ports via ``URL.create(port=…)``.
            raise ArgumentError(f"dqlite URL port {url.port!r} is out of the valid 1..65535 range")
        port = url.port or 9001
        database = url.database or "default"

        # Re-introduce IPv6 brackets that SA's parser strips: the client address
        # parser needs them to disambiguate ``[host]:port`` from a host literal.
        address = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        # Pre-validate host shape so a bad URL surfaces as ArgumentError from
        # create_engine, not a deferred InterfaceError. Import kept local so a
        # SA-only env without ``dqliteclient`` doesn't fail at module load.
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
            # Repeated keys arrive as a tuple; take the last occurrence.
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

        # One-shot WARNING when ``?max_total_rows=none`` disables the row-count cap
        # that guards against a malicious server's multi-GB result-set.
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
        """Reject any kwarg not in ``_CONNECT_KWARG_ALLOWED`` with ``ArgumentError``.

        Called from ``connect()`` so the merged ``cparams`` get the URL path's
        allowlist + per-key value validators, catching connect_args typos and
        out-of-range values (e.g. ``close_timeout=0.0001``) at first checkout.
        """
        # Dedicated AUTOCOMMIT rejection (vs the generic allowlist message), mirroring
        # the engine-level guard in ``__init__``.
        iso_level = kwargs.get("isolation_level")
        if isinstance(iso_level, str) and iso_level.upper() == "AUTOCOMMIT":
            raise ArgumentError(_AUTOCOMMIT_REJECTION_MSG)
        # Non-AUTOCOMMIT isolation_level: route via create_engine(isolation_level=),
        # not connect_args; give a directional message rather than a "typo" hint.
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

        Validates ``cparams`` against the allowlist before forwarding to
        ``dqlitedbapi.connect``. ``creator_fn`` is a dqlite-private sync hook
        (popped before validation) with no SA-canonical equivalent — prefer SA's
        ``create_engine(creator=...)``. The sync factory returns a usable
        Connection directly (no two-step connect like the async sibling).
        """
        creator_fn = cparams.pop("creator_fn", None)
        if creator_fn is not None:
            # Reject non-callable / async-def hooks with a precise ArgumentError
            # rather than leaking TypeError or returning a coroutine.
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

    # Truthful single-level lookup read by SA-internal by-key paths. Deliberately
    # diverges from ``get_isolation_level_values`` (which advertises AUTOCOMMIT as
    # a diagnostic-routing channel). Value 0 is never read (set_isolation_level is
    # fully overridden).
    _isolation_lookup = util.immutabledict({"SERIALIZABLE": 0})

    def get_isolation_level_values(
        self, dbapi_connection: DBAPIConnection
    ) -> Sequence[IsolationLevel]:
        """Return the isolation levels dqlite accepts: SERIALIZABLE only.

        AUTOCOMMIT is also advertised — not as accepted, but so SA passes it
        through to ``set_isolation_level``'s dedicated rejection message rather
        than its generic error. ``_isolation_lookup`` (truthful set) diverges.
        """
        return ["SERIALIZABLE", "AUTOCOMMIT"]

    def get_isolation_level(self, dbapi_connection: DBAPIConnection) -> IsolationLevel:
        """Return SERIALIZABLE (dqlite's only level)."""
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection: DBAPIConnection, level: IsolationLevel) -> None:
        """Accept SERIALIZABLE; reject everything else with ``ArgumentError``.

        AUTOCOMMIT gets the dedicated message; faking a weaker level would lose
        the semantics the caller asked for. ``None`` is strict-rejected (mirrors
        pysqlite's KeyError); the old silent-accept-None reset path no longer
        reaches here (``reset_isolation_level`` is now a no-op). Mostly
        defence-in-depth — SA validates against the values-list first.
        """
        # Case-insensitive: direct callers often pass lowercase.
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
        """SA pool-checkin reset hook — no-op (dqlite has no per-connection
        isolation state). The inherited reset would route an AUTOCOMMIT slot
        through ``set_isolation_level("AUTOCOMMIT")`` and raise from a finalize
        path the user did not initiate.
        """

    def on_connect(self) -> Callable[[DBAPIConnection], None]:
        """No-op (pysqlite's registers ``regexp`` / ``floor`` UDFs, which
        dqlitedbapi rejects — no UDF primitive). Returns a callable, not ``None``,
        to keep the parent's ``Callable[..., None]`` annotation.

        FK enforcement is already ON by default in dqlite (unlike pysqlite's OFF),
        so no hook is needed. To get SQLite's legacy unenforced behaviour, issue
        ``PRAGMA foreign_keys = OFF`` per connection, e.g.::

            from sqlalchemy import event


            @event.listens_for(engine, "connect")
            def _fk_pragma_off_on_connect(dbapi_connection, _):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=OFF")
                cursor.close()

        The dialect stays out of this choice (no URL knob to manage).
        """
        return lambda _conn: None

    def detect_autocommit_setting(self, dbapi_conn: DBAPIConnection) -> bool:
        """Always ``False`` so SA keeps managing BEGIN/COMMIT.

        The dbapi ``isolation_level`` stub returns ``None``; without this override
        SA's pysqlite-style probe would enable ``skip_autocommit_rollback`` (built
        for stdlib auto-BEGIN, which dqlite lacks) and silently break atomicity.
        Keep even if the dbapi probe later changes — this is the contract.
        """
        return False

    # ``dqlite_session_mode`` accepted values: "immediate" (default, writer-safe),
    # "deferred" (legacy, SNAPSHOT-vulnerable), "exclusive" (blocks readers),
    # "read_only" (PRAGMA query_only = 1). Routed through
    # DqliteSessionModeCharacteristic; unknown values raise at first checkout.
    _VALID_DQLITE_SESSION_MODES: ClassVar[frozenset[str]] = frozenset(
        {"immediate", "deferred", "exclusive", "read_only"}
    )

    @staticmethod
    def _unwrap_dqlite_connection(dbapi_connection: Any) -> Any:
        """Return the underlying dqlitedbapi connection (sync: it IS the conn;
        async: unwrap ``AsyncAdaptedConnection._connection``). Per-checkout state
        lives on the dqlitedbapi side.
        """
        inner = getattr(dbapi_connection, "_connection", None)
        return inner if inner is not None else dbapi_connection

    def _validate_dqlite_session_mode(self, mode: object) -> str:
        """Validate ``dqlite_session_mode``, returning the canonical lowercase form;
        ``ArgumentError`` otherwise (surfaces at first checkout, not first do_begin).
        """
        if not isinstance(mode, str):
            raise ArgumentError(f"dqlite_session_mode must be a str, got {type(mode).__name__}")
        normalised = mode.lower()
        if normalised not in self._VALID_DQLITE_SESSION_MODES:
            raise ArgumentError(
                f"Invalid dqlite_session_mode {mode!r}; "
                f"valid values are {sorted(self._VALID_DQLITE_SESSION_MODES)}"
            )
        return normalised

    # do_rollback / do_commit stay inherited (the "no active transaction" error is
    # swallowed at the dbapi layer). do_begin MUST be overridden: pysqlite's parent
    # is ``pass`` (stdlib auto-BEGINs), but dqlite has no auto-BEGIN — without an
    # explicit wire BEGIN every statement auto-commits and engine.begin() blocks
    # aren't atomic.
    #
    # Default emits bare BEGIN (the dbapi rewrites to BEGIN IMMEDIATE so
    # SELECT-then-INSERT can't lose its snapshot to a SQLITE_BUSY_SNAPSHOT).
    # ``dqlite_session_mode`` overrides the form per-session (see the values list
    # above); explicit literals bypass the dbapi rewrite.
    #
    # Asymmetric with the SAVEPOINT family (inherited, routed through SA's execute
    # pipeline which already classifies exceptions): do NOT mirror this raw-cursor
    # shape there.
    def do_begin(self, dbapi_connection: DBAPIConnection) -> None:
        target = self._unwrap_dqlite_connection(dbapi_connection)
        # Honour only a known str literal; any other shape (test MagicMock, etc.)
        # falls back to "immediate" — do_begin is the wrong place to surface misuse
        # (validation already ran at execution_options time).
        raw_mode = getattr(target, "_dqlite_session_mode", "immediate")
        mode = raw_mode.lower() if isinstance(raw_mode, str) and raw_mode else "immediate"
        if mode == "deferred" or mode == "read_only":
            # read_only rides DEFERRED (its PRAGMA query_only blocks writes); emit
            # the explicit literal so the contract is visible at the SA layer too.
            begin_sql = "BEGIN DEFERRED"
        elif mode == "exclusive":
            begin_sql = "BEGIN EXCLUSIVE"
        else:
            # "immediate" and any unknown value: bare BEGIN. The dbapi rewrites to
            # IMMEDIATE unless its engine-wide default (connect_args session_mode)
            # is non-immediate, in which case bare BEGIN is sent verbatim.
            begin_sql = "BEGIN"
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(begin_sql)
        finally:
            # Guard close so a transport-class failure doesn't replace the
            # propagating BEGIN exception (Python's finally-replaces rule) — SA's
            # wrapping must see the BEGIN error directly, not as __context__.
            try:
                cursor.close()
            except _FORCE_CLOSE_TAIL_EXCEPTIONS:
                # Wider tuple (matching do_close) so cross-loop RuntimeError /
                # dead-proxy ReferenceError don't escape and mask the BEGIN error;
                # programmer-bug shapes still escape.
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
        """Behavioural opt-out of any future ``DefaultDialect.do_executemany`` growth.

        Byte-equivalent to SA's one-line pass-through today; the override is the
        contract that dqlite does NOT inherit future wrapper logic. Body MUST stay
        byte-equivalent (pinned by ``test_do_executemany_local_override_pin``).
        """
        cursor.executemany(statement, parameters)

    def do_execute(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any = None,
    ) -> None:
        """Behavioural opt-out, sibling of :meth:`do_executemany`. Body MUST stay
        byte-equivalent to SA's pass-through (pinned by
        ``test_do_execute_local_override_pin``).
        """
        cursor.execute(statement, parameters)

    def do_execute_no_params(
        self,
        cursor: Any,
        statement: str,
        context: Any = None,
    ) -> None:
        """Behavioural opt-out, sibling of :meth:`do_executemany`. Body MUST stay
        byte-equivalent to SA's pass-through (pinned by
        ``test_do_execute_local_override_pin``).
        """
        cursor.execute(statement)

    # Disconnect substring patterns, stored lower-case (matched case-insensitively).
    _dqlite_disconnect_messages = (
        "connection closed",
        # Async remap of RuntimeError("Event loop is closed") — asyncio dead.
        "event loop closed",
        "timed out",
        # Derived from the dbapi prefix constant so both sides stay in lockstep.
        _DBAPI_FAILED_TO_CONNECT_PREFIX.rstrip(": ").lower(),
        "not connected",
        # Wire-layer desync (ProtocolError wrapped as OperationalError(code=None));
        # shared ``dqlitewire`` constant, recognised here.
        WIRE_DECODE_FAILED_PREFIX,
        # Cross-loop misuse remapped by aio._handle_exception to this canonical
        # prefix (the bare "different loop" substrings would false-positive).
        "event-loop mismatch:",
        # Nested-loop RuntimeError remap: first matches the remap prefix verbatim
        # (refactor-safe), second matches the appended original-error cause text.
        "event loop already running",
        "loop is already running",
    )

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """Detect whether an exception indicates a broken connection.

        Prefer type dispatch over message matching (the C server's wording is not
        a contract); walks the full cause chain so wrapped errors aren't missed.
        Asymmetry with ``do_ping``: ``do_ping`` treats ProgrammingError as a dead
        slot (its only op is SELECT 1), but here a bare ProgrammingError is likely
        a caller bug and must propagate. Cross-loop faults are remapped to
        ``OperationalError("event-loop mismatch: ...")`` upstream.
        """
        # Single cause-chain walk classifying each node. Order matters:
        # ClusterPolicyError short-circuits before its parent ClusterError; the
        # leader-change code check runs before the substring scan (so a coded
        # leader-flip isn't gated out by the code-is-None restriction); the
        # substring scan is the sole classifier for code=None and must come last.
        # Substring scan is restricted to OperationalError(code=None) and bare
        # DatabaseError(11/24/26) — a server RAISE() in a coded error must not trip
        # disconnect-and-retry (would duplicate non-idempotent INSERTs). Reads
        # ``raw_message`` first (full server text; ``str(cause)`` is truncated).
        for cause in _walk_cause_chain(e):
            # ClusterPolicyError first (subclasses ClusterError): deterministic
            # config rejection, NOT a disconnect.
            if isinstance(cause, _client_exc.ClusterPolicyError):
                return False
            if isinstance(cause, (_client_exc.DqliteConnectionError, _client_exc.ClusterError)):
                return True
            # Bare client ProtocolError (e.g. from a dbapi-bypassing middleware)
            # before it's wrapped as OperationalError.
            if isinstance(cause, _client_exc.ProtocolError):
                return True
            # OS-level transport faults anywhere in the chain (the entry node is
            # yielded at depth 0, so this also covers a bare OSError / group child).
            if isinstance(cause, OSError):
                return True
            # Closed-handle / loop-binding / invalidated / fork InterfaceError
            # surfaces. Match raw_message first (full server text).
            if isinstance(cause, _dbapi_exc.InterfaceError):
                raw = getattr(cause, "raw_message", None) or str(cause)
                message = raw.lower()
                if "connection is closed" in message or "cursor is closed" in message:
                    return True
                # Loop-binding faults (closed/different loop GC'd): slot is dead.
                # Anchored to "is bound to a" so an unrelated user/OTel message
                # mentioning event loops doesn't false-positive.
                if (
                    "is bound to a closed event loop" in message
                    or "is bound to a different event loop" in message
                ):
                    return True
                # Cancel-after-invalidate; the "(id=" lexeme is the dbapi's contract
                # so a user RAISE without it doesn't false-positive.
                if "connection invalidated (id=" in message:
                    return True
                # Fork-inherited slot (parent's loop/lock state unusable); lets
                # gunicorn-preload / Celery-prefork deployments self-heal.
                if "used after fork" in message:
                    return True
            # Leader-change code, checked before the substring scan so a coded
            # leader-flip isn't gated out by the code-is-None restriction.
            for err_class in (_dbapi_exc.OperationalError, _client_exc.OperationalError):
                if (
                    isinstance(cause, err_class)
                    and getattr(cause, "code", None) in LEADER_ERROR_CODES
                ):
                    return True
                # Go-parity errNotFound→ErrBadConn: SQLITE_NOTFOUND (12) is
                # overloaded (leader-flip LOOKUP_DB vs the stmt-id state bug).
                # Substring-gate on the wire-side leader-lost prefix so only the
                # leader-flip arm invalidates.
                if (
                    isinstance(cause, err_class)
                    and getattr(cause, "code", None) == _SQLITE_NOTFOUND
                ):
                    raw = (
                        getattr(cause, "raw_message", None) or getattr(cause, "message", None) or ""
                    )
                    msg_lc = raw.lower()
                    if msg_lc.startswith(LEADER_LOST_DB_LOOKUP_SUBSTRING):
                        return True
            # Substring scan, restricted to OperationalError(code=None).
            if isinstance(cause, _dbapi_exc.OperationalError):
                cause_code = getattr(cause, "code", None)
                applies_substring = cause_code is None
                # Narrow second channel for the translateRaftErrCode collapse:
                # cluster-mgmt RAFT errors arrive as code=1 with raft_strerror text;
                # match the bounded marker set without reopening the broad scan.
                if not applies_substring and cause_code == _SQLITE_ERROR_CODE:
                    text = getattr(cause, "raw_message", None) or str(cause)
                    msg_lower = text.lower()
                    for marker in _RAFT_COLLAPSE_DISCONNECT_MARKERS:
                        if marker in msg_lower:
                            return True
            elif type(cause) is _dbapi_exc.DatabaseError:
                # ``type is`` (NOT isinstance) so only the BARE DatabaseError from
                # _classify_operational (codes 11/24/26) reaches here — subclasses
                # (caller-side bugs) must propagate, not invalidate-and-retry.
                if getattr(cause, "code", None) in _BARE_DBE_DISCONNECT_CODES:
                    # Slot-fatal regardless of message (the canonical CORRUPT/FORMAT/
                    # NOTADB wordings are deliberately not in _dqlite_disconnect_messages).
                    return True
                applies_substring = False
            elif isinstance(cause, _dbapi_exc.InterfaceError):
                # Reached only when the dedicated-phrase arm above didn't match: a
                # server-emitted InterfaceError under a disconnect-eligible code.
                # The substring patterns here are disjoint from those phrases.
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
        # Do NOT delegate to super(): its check is the in-process sqlite3 surface
        # (already subsumed) and dereferences ``self.dbapi`` (None until initialize).
        return False

    # Two-phase commit unsupported (no XA coordinator). Surface PEP 249
    # NotSupportedError (not the inherited NotImplementedError) so callers can
    # catch it uniformly. The compliance suite already skips these via requirements.
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
        # ``NoReturn`` (not ``list[Any]``) so type-checkers flag downstream code
        # as unreachable — the body unconditionally raises.
        raise _dbapi_exc.NotSupportedError("dqlite does not support two-phase commit.")

    def do_close(self, dbapi_connection: Any) -> None:
        """SA pool checkin / ``engine.dispose()`` graceful close path.

        Calls the dbapi's ``Connection.close()``; on a transport-class failure or
        the documented cross-loop / dead-proxy raises, falls back to
        ``force_close_transport()`` so the slot still releases. Programmer bugs
        propagate. Both arms use the wider ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` so a
        first-close ``RuntimeError("Event loop is closed")`` lands on the fallback
        rather than escaping (honouring the "do_close never raises" invariant).
        """
        try:
            dbapi_connection.close()
        except _FORCE_CLOSE_TAIL_EXCEPTIONS:
            logger.debug(
                "do_close: graceful close raised transport-class error; "
                "falling back to force_close_transport",
                exc_info=True,
            )
            with contextlib.suppress(*_FORCE_CLOSE_TAIL_EXCEPTIONS):
                dbapi_connection.force_close_transport()

    def do_terminate(self, dbapi_connection: Any) -> None:
        """Force-close the connection without awaiting in-flight ops (SA's bounded
        forced-reclaim path, promised by ``has_terminate = True``).

        Routes through the dbapi's ``force_close_transport`` (bounded by
        close_timeout ~0.5 s), unlike do_close's ~10 s wire-read window. Suppresses
        all tail ``Exception`` (non-raising contract) but NOT ``BaseException``
        (KeyboardInterrupt / SystemExit must propagate). Two-tier: expected
        transport shapes DEBUG-log; unexpected (likely a dbapi refactor regression)
        WARNING-log.
        """
        peer = _log_safe_peer(dbapi_connection)
        try:
            dbapi_connection.force_close_transport()
        except _FORCE_CLOSE_TAIL_EXCEPTIONS:
            # Expected transport-class shapes — DEBUG-log + absorb.
            logger.debug(
                "do_terminate: force_close_transport raised on dispose for "
                "peer=%s id=%s; proceeding (has_terminate=True non-raising "
                "contract)",
                peer,
                id(dbapi_connection),
                exc_info=True,
            )
        except Exception:
            # Unexpected shape — likely a dbapi refactor regression. Absorb (the
            # non-raising contract) but WARNING-tier so it stays visible.
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

        Only connection-level exceptions mean "dead"; anything else propagates so
        real bugs aren't rewritten as "reconnect". Bare ``DatabaseError`` for codes
        11/24/26 (CORRUPT/FORMAT/NOTADB) is a ping-failure so the pool tries another
        node.
        """
        # ``cursor()`` is inside the try because it too can raise on a stale slot
        # (OSError / closed InterfaceError); OSError is not a dbapi.Error subclass
        # and would otherwise escape past the ping caller's filter.
        cursor: Any = None
        try:
            try:
                cursor = dbapi_connection.cursor()
                cursor.execute(self._dialect_specific_select_one)
                return True
            except (
                # Per-class (not the DatabaseError umbrella) so Integrity / Data /
                # Internal / NotSupported from a buggy SELECT-trigger propagate.
                # Any InterfaceError here means the slot is unusable — more
                # conservative than is_disconnect's real-query classification (which
                # lets caller-side bind misuse surface).
                _dbapi_exc.OperationalError,
                _dbapi_exc.ProgrammingError,
                _dbapi_exc.InterfaceError,
                _client_exc.DqliteConnectionError,
                OSError,
            ):
                return False
            except _dbapi_exc.DatabaseError as exc:
                # Bare DatabaseError(11/24/26): ping-fail so the pool invalidates;
                # other coded subclasses propagate.
                if getattr(exc, "code", None) in _BARE_DBE_DISCONNECT_CODES:
                    return False
                raise
        finally:
            # Same suppression set as the outer except (programming bugs still
            # propagate); cursor is None when cursor() itself raised.
            if cursor is not None:
                try:
                    cursor.close()
                except (
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

    # ``_get_server_version_info`` is inherited (reads dqlitedbapi's pinned
    # ``sqlite_version_info``, the SQLite floor the project guarantees). SA gates
    # feature dispatch on it — operators must run cluster SQLite >= the floor.


# DDL kwarg-prefix runtime guard: SA stores ``dqlite_*`` kwargs under
# ``dialect_options['dqlite']``, but ``SQLiteDDLCompiler`` reads only
# ``['sqlite']``, so they'd be silently dropped. Raise at construction instead.
def _dqlite_prefix_ddl_guard(target: Any, parent: Any) -> None:
    dqlite_opts = target.dialect_options.get("dqlite")
    if dqlite_opts is None:
        return
    # ``_non_defaults`` is SA's "user actually passed this" set (the public view
    # includes registry defaults), so the guard doesn't fire on inherited defaults.
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
    """Register the prefix guard on the SQLite DDL constructs; idempotent."""
    from sqlalchemy import event
    from sqlalchemy.sql import schema as sa_schema

    _ATTR = "_dqlite_prefix_guard_installed"
    if getattr(_install_dqlite_prefix_ddl_guard, _ATTR, False):
        return
    for cls in (sa_schema.Table, sa_schema.Index, sa_schema.Column, sa_schema.Constraint):
        event.listen(cls, "after_parent_attach", _dqlite_prefix_ddl_guard)
    setattr(_install_dqlite_prefix_ddl_guard, _ATTR, True)


_install_dqlite_prefix_ddl_guard()
