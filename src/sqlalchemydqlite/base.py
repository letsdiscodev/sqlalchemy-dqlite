"""Base dqlite dialect for SQLAlchemy."""

import datetime
import logging
import math
import types
import warnings
from collections.abc import Callable, Sequence
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
    """Passthrough DateTime — ``dqlitedbapi`` already returns ``datetime.datetime``
    for columns declared as DATETIME/TIMESTAMP (matching PEP 249 and the
    psycopg/mysqlclient convention), so no string parsing is needed.

    Inheriting from ``sqltypes.DateTime`` (not ``sqlite.DATETIME``) is
    deliberate: the generic parent's ``literal_processor`` calls
    ``value.isoformat()`` directly — bypassing pysqlite's
    iso-string-based bind processor that would double-convert our
    already-datetime values. The parent's default
    ``bind_processor`` / ``result_processor`` return ``None`` already,
    so no explicit overrides are needed here.
    """


class _DqliteDate(sqltypes.Date):
    """Passthrough Date — ``dqlitedbapi`` returns ``datetime.datetime`` for
    DATE columns (the C server tags all of DATETIME/DATE/TIMESTAMP as
    ``DQLITE_ISO8601``); narrow to ``datetime.date`` on read.

    A tz-aware input datetime has its tzinfo silently dropped by
    ``.date()`` (``datetime.date`` has no tz support). The returned
    date is the UTC-day portion when the dbapi decoded an ISO8601
    value — not the viewer's local day. Applications that care about
    local-day semantics should store DATETIME instead and do the
    narrowing themselves.
    """

    def bind_processor(self, dialect: Any) -> None:
        return None

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if isinstance(value, datetime.datetime):
                # Deliberate: tzinfo is dropped. See class docstring.
                return value.date()
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
    # wire codec emits ``bytes`` for ``ValueType.BLOB``, and the
    # PEP 249 ``Binary()`` constructor returns ``bytes``). Pin True
    # locally so ``LargeBinary.result_processor`` can skip the redundant
    # ``bytes(value)`` wrap on every BLOB cell.
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
    _URL_QUERY_ALLOWED: dict[str, tuple[Callable[[str], Any], Callable[[Any], bool] | None]] = {
        "timeout": (float, lambda v: math.isfinite(v) and v > 0),
        "max_total_rows": (int, lambda v: v > 0),
        "max_continuation_frames": (int, lambda v: v > 0),
        "trust_server_heartbeat": (
            lambda s: _parse_url_bool("trust_server_heartbeat", s),
            None,
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
        """
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

        dqlite only supports SERIALIZABLE. ``AUTOCOMMIT`` is explicitly
        rejected because silently dropping the request would cause users
        to lose transactionality without knowing it. Other unsupported
        levels emit a warning (future-proof for isolation levels dqlite
        may grow to support).

        Note on reachability: SA's engine flow
        (``engine/default.py::_assert_and_set_isolation_level``) calls
        ``get_isolation_level_values()`` first and rejects unknown
        values with ``ArgumentError`` before reaching this method, so
        the AUTOCOMMIT and warning branches are effectively dead for
        engine-driven callers. They are kept as belt-and-suspenders
        for third-party callers (test harnesses, custom engine
        implementations) that bypass SA's upstream validation, and to
        provide an explicit error message if the guarantees above
        ever change.
        """
        if level is None or level == "SERIALIZABLE":
            return
        if level == "AUTOCOMMIT":
            raise ArgumentError(
                "dqlite does not support AUTOCOMMIT; every statement goes through "
                "Raft consensus and there is no per-statement autocommit mode. "
                "Use explicit commit() / rollback() on the connection."
            )
        warnings.warn(
            f"dqlite only supports SERIALIZABLE isolation. Requested level {level!r} is ignored.",
            stacklevel=2,
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
    )

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """Detect whether an exception indicates a broken connection.

        Prefer exception-type dispatch over message matching; the C
        server's error wording is not a contract. Type-based checks
        cover TCP resets, DNS failures, and partial-read timeouts that
        the hand-maintained substring list misses.
        """
        # Explicit connection-level error types from the client layer.
        if isinstance(e, _client_exc.DqliteConnectionError):
            return True
        # The dbapi ``_call_client`` handler wraps
        # ``_client_exc.DqliteConnectionError`` into a bare
        # ``dbapi.OperationalError`` (no code). The wrapped class is
        # unreachable by the direct isinstance above, but Python sets
        # ``__cause__`` from ``raise ... from e``, so walking the chain
        # keeps the disconnect classification working without inventing
        # a new attribute.
        if isinstance(getattr(e, "__cause__", None), _client_exc.DqliteConnectionError):
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
        constant pinning the minimum supported SQLite version). The
        previous implementation ran a live ``SELECT sqlite_version()``
        on every fresh engine connection and fell back to ``(3, 0, 0)``
        on any transient error — which silently disabled RETURNING /
        multi-values / all 3.35+ features on the affected engine. The
        DBAPI constant is authoritative and matches how pysqlite
        implements the same override.
        """
        info = getattr(self.dbapi, "sqlite_version_info", None)
        if info is not None:
            return tuple(info)
        return (3, 35, 0)


# Register the dialect
dialect = DqliteDialect
