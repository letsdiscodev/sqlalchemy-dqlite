"""SQLAlchemy dialect for dqlite, built on the pysqlite dialect and dqlite-dbapi."""

import contextlib
import datetime
import inspect
import logging
import math
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from typing import Any, ClassVar, Final

from sqlalchemy import event, pool, util
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite.base import SQLiteCompiler
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import URL
from sqlalchemy.engine import characteristics as sa_characteristics
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql import schema as sa_schema
from sqlalchemy.util import await_only

import dqliteclient.exceptions as client_exc
import dqlitedbapi
import dqlitedbapi.exceptions as dbapi_exc
from dqliteclient import CLOSE_TIMEOUT_FLOOR, parse_address
from dqlitedbapi import SESSION_MODES
from dqlitedbapi.types import format_utc_offset
from dqlitewire import BARE_DATABASE_ERROR_CODES, LEADER_ERROR_CODES, sanitize_for_log

__all__ = ["DqliteCompiler", "DqliteDialect", "DqliteSessionModeCharacteristic"]

logger = logging.getLogger(__name__)

AUTOCOMMIT_REJECTED: Final[str] = (
    "dqlite does not support SA's AUTOCOMMIT isolation level; the dialect always brackets "
    "statements in BEGIN / COMMIT. Use explicit commit() / rollback() on the connection."
)
TWOPHASE_REJECTED: Final[str] = "dqlite does not support two-phase commit."

# Server text the C server emits under SQLITE_ERROR when raft, not SQLite, failed.
_RAFT_DISCONNECT_MARKERS: Final[tuple[str, ...]] = (
    "server is shutting down",
    "operation canceled",
    "no connection to remote server",
)
# dbapi InterfaceError wordings that mean the slot is unusable (closed, lost, or
# reached from the wrong loop or process).
_INTERFACE_DISCONNECT_MARKERS: Final[tuple[str, ...]] = (
    "connection is closed",
    "cursor is closed",
    "connection invalidated (id=",
    "used after fork",
    "is bound to a",
)
# dbapi OperationalError wordings for transport failures raised without a cause chain.
_OPERATIONAL_DISCONNECT_MARKERS: Final[tuple[str, ...]] = (
    "connection closed",
    "not connected",
    "timed out",
    "failed to connect",
    "wire decode failed",
    "event loop",
)
_MAX_INT32: Final[int] = 2**31 - 1


# -- connect argument validation -----------------------------------------------------


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _parse_int_or_none(text: str) -> int | None:
    if text.strip().lower() == "none":
        return None
    return int(text)


def _parse_bool(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"expected a boolean, got {text!r}")


def _timeout(minimum: float = 0.0) -> Callable[[object], bool]:
    return lambda v: _is_number(v) and v > 0 and v >= minimum  # type: ignore[operator]


def _int_cap(upper: int) -> Callable[[object], bool]:
    return lambda v: v is None or (_is_int(v) and 0 < v <= upper)  # type: ignore[operator]


# name -> (converter from URL text, validator of the typed value)
CONNECT_PARAMS: Final[dict[str, tuple[Callable[[str], Any], Callable[[object], bool]]]] = {
    "timeout": (float, _timeout()),
    "dial_timeout": (float, _timeout()),
    "attempt_timeout": (float, _timeout()),
    "close_timeout": (float, _timeout(CLOSE_TIMEOUT_FLOOR)),
    "busy_timeout": (float, lambda v: _is_number(v) and v >= 0),  # type: ignore[operator]
    "max_total_rows": (_parse_int_or_none, _int_cap(_MAX_INT32)),
    "max_continuation_frames": (_parse_int_or_none, _int_cap(_MAX_INT32)),
    "max_message_size": (_parse_int_or_none, _int_cap(_MAX_INT32)),
    "trust_server_heartbeat": (_parse_bool, lambda v: isinstance(v, bool)),
    "check_same_thread": (_parse_bool, lambda v: isinstance(v, bool)),
    "session_mode": (str.lower, lambda v: isinstance(v, str) and v.lower() in SESSION_MODES),
}
CONNECT_ARGS_ALLOWED: Final[frozenset[str]] = frozenset(
    {*CONNECT_PARAMS, "address", "database", "dial_func"}
)


def validate_connect_args(kwargs: dict[str, Any]) -> None:
    """Reject unknown or out-of-range ``connect_args`` with :class:`ArgumentError`."""
    if "isolation_level" in kwargs:
        level = kwargs["isolation_level"]
        if isinstance(level, str) and level.upper() == "AUTOCOMMIT":
            raise ArgumentError(AUTOCOMMIT_REJECTED)
        raise ArgumentError(
            "pass isolation_level to create_engine(), not connect_args (dqlite only "
            "supports SERIALIZABLE)"
        )
    unknown = sorted(set(kwargs) - CONNECT_ARGS_ALLOWED)
    if unknown:
        raise ArgumentError(
            f"Unknown dqlite connect kwarg(s) {unknown!r}. Allowed: {sorted(CONNECT_ARGS_ALLOWED)}"
        )
    for key, value in kwargs.items():
        if key in CONNECT_PARAMS and not CONNECT_PARAMS[key][1](value):
            raise ArgumentError(f"connect_args value for {key!r} = {value!r} is out of range")


def _walk_causes(exc: BaseException, limit: int = 64) -> Iterator[BaseException]:
    """``exc`` and everything reachable through ``__cause__``, ``__context__`` and groups."""
    seen: set[int] = set()
    queue: deque[BaseException] = deque([exc])
    while queue and len(seen) < limit:
        current = queue.popleft()
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        queue.extend(c for c in (current.__cause__, current.__context__) if c is not None)
        if isinstance(current, BaseExceptionGroup):
            queue.extend(current.exceptions)


# -- temporal types --------------------------------------------------------------------
#
# dqlite-dbapi returns typed date/time cells as datetime objects and TEXT cells as str;
# pysqlite's processors only understand str. Binds keep pysqlite's fixed six-digit
# fraction so values written by earlier releases compare equal.


def _iso_datetime(value: datetime.datetime) -> str:
    text = (
        f"{value.year:04d}-{value.month:02d}-{value.day:02d} "
        f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"
    )
    offset = value.utcoffset()
    return text if offset is None else text + format_utc_offset(offset)


def _iso_time(value: datetime.time) -> str:
    text = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"
    offset = value.utcoffset()
    return text if offset is None else text + format_utc_offset(offset)


def _parse_text(value: str, parser: Callable[[str], Any], kind: str) -> Any:
    try:
        return parser(value)
    except ValueError:
        logger.debug("%s column holds unparseable text %r", kind, sanitize_for_log(value)[:200])
        return value


class _DqliteDateTime(sqltypes.DateTime):
    def bind_processor(self, dialect: Any) -> Callable[[Any], Any]:
        def process(value: Any) -> Any:
            if isinstance(value, datetime.datetime):
                return _iso_datetime(value)
            if isinstance(value, datetime.time):
                raise dbapi_exc.DataError(
                    f"DateTime column cannot bind a time-only value {value!r}"
                )
            if isinstance(value, datetime.date):
                return _iso_datetime(datetime.datetime.combine(value, datetime.time()))
            return value

        return process

    def literal_processor(self, dialect: Any) -> Callable[[Any], str]:
        bind = self.bind_processor(dialect)
        return lambda value: "NULL" if value is None else f"'{bind(value)}'"

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any]:
        want_timezone = self.timezone

        def process(value: Any) -> Any:
            if isinstance(value, str):
                value = _parse_text(value, datetime.datetime.fromisoformat, "DateTime")
            if isinstance(value, datetime.datetime):
                if want_timezone:
                    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)
                if value.tzinfo is not None:
                    return value.astimezone(datetime.UTC).replace(tzinfo=None)
                return value
            if isinstance(value, datetime.time):
                raise dbapi_exc.DataError(f"DateTime column holds a time-only value {value!r}")
            return value

        return process


class _DqliteDate(sqltypes.Date):
    def bind_processor(self, dialect: Any) -> Callable[[Any], Any]:
        def process(value: Any) -> Any:
            if isinstance(value, datetime.datetime):
                return value.date()
            if isinstance(value, datetime.time):
                raise dbapi_exc.DataError(f"Date column cannot bind a time-only value {value!r}")
            return value

        return process

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any]:
        def process(value: Any) -> Any:
            if isinstance(value, str):
                return _parse_text(value, datetime.date.fromisoformat, "Date")
            if isinstance(value, datetime.datetime):
                return value.date()
            if isinstance(value, datetime.time):
                raise dbapi_exc.DataError(f"Date column holds a time-only value {value!r}")
            return value

        return process


class _DqliteTime(sqltypes.Time):
    def bind_processor(self, dialect: Any) -> Callable[[Any], Any]:
        def process(value: Any) -> Any:
            if isinstance(value, datetime.time):
                return _iso_time(value)
            if isinstance(value, datetime.date):
                raise dbapi_exc.DataError(
                    f"Time column cannot bind {value!r}; narrow to .time() / .timetz()"
                )
            return value

        return process

    def literal_processor(self, dialect: Any) -> Callable[[Any], str]:
        bind = self.bind_processor(dialect)
        return lambda value: "NULL" if value is None else f"'{bind(value)}'"

    def result_processor(self, dialect: Any, coltype: Any) -> Callable[[Any], Any]:
        want_timezone = self.timezone

        def process(value: Any) -> Any:
            if isinstance(value, str):
                value = _parse_text(value, datetime.time.fromisoformat, "Time")
            if isinstance(value, datetime.datetime):
                value = value.timetz()
            if isinstance(value, datetime.time):
                if want_timezone:
                    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)
                return value.replace(tzinfo=None) if value.tzinfo else value
            return value

        return process


# -- compiler and characteristics ----------------------------------------------------


class DqliteCompiler(SQLiteCompiler):
    """Refuses at compile time what pysqlite backs with Python-side UDFs."""

    def visit_regexp_match_op_binary(self, binary: Any, operator: Any, **kw: Any) -> str:
        raise dbapi_exc.NotSupportedError(
            "regexp_match is not available on dqlite: the wire protocol has no UDF "
            "primitive, so SQLite's REGEXP operator cannot be provided. Use LIKE instead."
        )

    def visit_not_regexp_match_op_binary(self, binary: Any, operator: Any, **kw: Any) -> str:
        return self.visit_regexp_match_op_binary(binary, operator, **kw)

    def visit_function(self, func: Any, add_to_result_map: Any = None, **kwargs: Any) -> str:
        if str(getattr(func, "name", "")).lower() == "floor":
            raise dbapi_exc.NotSupportedError(
                "floor() requires a dqlite server built with SQLITE_ENABLE_MATH_FUNCTIONS; the "
                "dialect cannot register it as a UDF. Use CAST(col AS INTEGER) instead."
            )
        return super().visit_function(func, add_to_result_map, **kwargs)


def unwrap_dbapi_connection(dbapi_connection: Any) -> Any:
    """The dqlite-dbapi connection behind ``dbapi_connection`` (async connections are
    wrapped by the SQLAlchemy adapter; sync ones are used directly)."""
    inner = getattr(dbapi_connection, "_connection", None)
    return dbapi_connection if inner is None else inner


def validate_session_mode(mode: object) -> str:
    try:
        return dqlitedbapi.validate_session_mode(mode)
    except (TypeError, ValueError) as exc:
        raise ArgumentError(f"dqlite_session_mode: {exc}") from exc


class DqliteSessionModeCharacteristic(sa_characteristics.ConnectionCharacteristic):
    """The ``dqlite_session_mode`` execution option, backed by the dbapi connection's
    ``session_mode`` (how a bare ``BEGIN`` is qualified, or read-only)."""

    transactional: ClassVar[bool] = True

    def get_characteristic(self, dialect: Any, dbapi_conn: Any) -> str:
        return str(unwrap_dbapi_connection(dbapi_conn).session_mode)

    def set_characteristic(self, dialect: Any, dbapi_conn: Any, value: Any) -> None:
        result = unwrap_dbapi_connection(dbapi_conn).set_session_mode(validate_session_mode(value))
        if inspect.isawaitable(result):
            await_only(result)

    def reset_characteristic(self, dialect: Any, dbapi_conn: Any) -> None:
        target = unwrap_dbapi_connection(dbapi_conn)
        self.set_characteristic(dialect, dbapi_conn, target.default_session_mode)


# -- dialect -----------------------------------------------------------------------------


class DqliteDialect(SQLiteDialect_pysqlite):
    """``dqlite://host:port/database``. Only ``SERIALIZABLE`` isolation; DDL kwargs use
    the ``sqlite_*`` prefix. See docs/ for the URL parameters and transaction model."""

    name = "dqlite"
    driver = "dqlitedbapi"
    supports_statement_cache = True
    is_async = False
    has_terminate = True

    supports_server_side_cursors = False
    supports_native_boolean = True
    non_native_boolean_check_constraint = False
    insert_executemany_returning = True
    update_executemany_returning = True
    delete_executemany_returning = True
    default_isolation_level = "SERIALIZABLE"
    _isolation_lookup = util.immutabledict({"SERIALIZABLE": 0})

    statement_compiler = DqliteCompiler
    colspecs = {
        **SQLiteDialect_pysqlite.colspecs,
        sqltypes.DateTime: _DqliteDateTime,
        sqltypes.TIMESTAMP: _DqliteDateTime,
        sqltypes.Date: _DqliteDate,
        sqltypes.Time: _DqliteTime,
    }
    connection_characteristics = util.immutabledict(
        {
            **SQLiteDialect_pysqlite.connection_characteristics,
            "dqlite_session_mode": DqliteSessionModeCharacteristic(),
        }
    )

    def __init__(self, **kwargs: Any) -> None:
        paramstyle = kwargs.get("paramstyle")
        if paramstyle is not None and paramstyle != "qmark":
            raise ArgumentError(f"dqlite dialect requires paramstyle='qmark'; got {paramstyle!r}")
        level = kwargs.get("isolation_level")
        if isinstance(level, str) and level.upper() == "AUTOCOMMIT":
            raise ArgumentError(AUTOCOMMIT_REJECTED)
        if "native_datetime" in kwargs:
            raise ArgumentError(
                "dqlite dialect does not honour native_datetime; dates are exchanged as ISO 8601"
            )
        super().__init__(**kwargs)

    @classmethod
    def import_dbapi(cls) -> Any:
        return dqlitedbapi

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return pool.QueuePool

    # -- connecting ------------------------------------------------------------------

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """``dqlite://host:port/database?param=value``; see docs/connection-url.md."""
        if url.username is not None or url.password is not None:
            raise ArgumentError("Invalid URL: dqlite does not accept username or password")
        host = url.host or "localhost"
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ArgumentError(f"dqlite URL port {url.port!r} is out of the valid 1..65535 range")
        port = url.port or 9001
        address = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        try:
            parse_address(address)
        except ValueError as exc:
            raise ArgumentError(f"Invalid dqlite URL host: {exc}") from exc
        database = url.database or "default"
        if "#" in database:
            raise ArgumentError(f"Invalid URL: dqlite does not accept fragments ({database!r})")
        kwargs: dict[str, Any] = {"address": address, "database": database}
        for key, raw in url.query.items():
            text = raw[-1] if isinstance(raw, tuple) else raw
            if key not in CONNECT_PARAMS:
                raise ArgumentError(
                    f"Unknown dqlite URL query parameter {key!r}. Allowed: {sorted(CONNECT_PARAMS)}"
                )
            if "#" in text:
                raise ArgumentError(
                    f"Invalid URL: dqlite does not accept fragments ({key}={text!r})"
                )
            converter, validator = CONNECT_PARAMS[key]
            try:
                value = converter(text)
            except (TypeError, ValueError) as exc:
                raise ArgumentError(f"Cannot convert URL query {key}={text!r}: {exc}") from exc
            if not validator(value):
                raise ArgumentError(f"URL query {key}={text!r} is out of range")
            kwargs[key] = value
        if kwargs.get("max_total_rows", 0) is None:
            logger.warning(
                "dqlite: max_total_rows cap disabled via URL; the client will accept "
                "arbitrarily large result sets"
            )
        return [], kwargs

    _creator_kwarg = "creator_fn"

    def _pop_creator(self, cparams: dict[str, Any]) -> Callable[..., Any] | None:
        creator = cparams.pop(self._creator_kwarg, None)
        if creator is None:
            return None
        if not callable(creator) or inspect.iscoroutinefunction(creator):
            raise ArgumentError(
                f"connect_args[{self._creator_kwarg!r}] must be callable and synchronous, "
                "returning a dbapi connection (the async engine, dqlite+aio://, awaits the "
                "connection's connect() itself)"
            )
        return creator  # type: ignore[no-any-return]

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        creator = self._pop_creator(cparams)
        validate_connect_args(cparams)
        if creator is not None:
            return creator(*cargs, **cparams)
        return self.loaded_dbapi.connect(*cargs, **cparams)

    def on_connect(self) -> None:  # type: ignore[override]
        return None  # pysqlite registers regexp/floor UDFs; dqlite has no UDF primitive

    # -- isolation and transactions ------------------------------------------------------

    def get_isolation_level_values(
        self, dbapi_connection: DBAPIConnection
    ) -> Sequence[IsolationLevel]:
        # AUTOCOMMIT is listed so SA routes it to our dedicated rejection message.
        return ["SERIALIZABLE", "AUTOCOMMIT"]

    def get_isolation_level(self, dbapi_connection: DBAPIConnection) -> IsolationLevel:
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection: DBAPIConnection, level: IsolationLevel) -> None:
        normalised = level.upper() if isinstance(level, str) else level
        if normalised == "SERIALIZABLE":
            return
        if normalised == "AUTOCOMMIT":
            raise ArgumentError(AUTOCOMMIT_REJECTED)
        raise ArgumentError(f"dqlite only supports SERIALIZABLE isolation; got {level!r}")

    def reset_isolation_level(self, dbapi_connection: DBAPIConnection) -> None:
        return None

    def detect_autocommit_setting(self, dbapi_conn: DBAPIConnection) -> bool:
        return False

    def do_begin(self, dbapi_connection: DBAPIConnection) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("BEGIN")  # the dbapi qualifies it per session_mode
        finally:
            with contextlib.suppress(dbapi_exc.Error, client_exc.DqliteError, OSError):
                cursor.close()

    def do_begin_twophase(self, connection: Any, xid: Any) -> None:
        raise dbapi_exc.NotSupportedError(TWOPHASE_REJECTED)

    def do_prepare_twophase(self, connection: Any, xid: Any) -> None:
        raise dbapi_exc.NotSupportedError(TWOPHASE_REJECTED)

    def do_commit_twophase(
        self, connection: Any, xid: Any, is_prepared: bool = True, recover: bool = False
    ) -> None:
        raise dbapi_exc.NotSupportedError(TWOPHASE_REJECTED)

    def do_rollback_twophase(
        self, connection: Any, xid: Any, is_prepared: bool = True, recover: bool = False
    ) -> None:
        raise dbapi_exc.NotSupportedError(TWOPHASE_REJECTED)

    def do_recover_twophase(self, connection: Any) -> list[Any]:
        raise dbapi_exc.NotSupportedError(TWOPHASE_REJECTED)

    # -- liveness --------------------------------------------------------------------

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """Type-driven: the dbapi wraps every client failure with the original as its
        cause, so the chain, not the message, says whether the wire session is gone."""
        if connection is not None:
            inner = unwrap_dbapi_connection(connection)
            if getattr(inner, "invalidated", False) or getattr(inner, "closed", False):
                return True
        for cause in _walk_causes(e):
            if isinstance(cause, client_exc.ClusterPolicyError):
                return False
            if isinstance(
                cause,
                client_exc.DqliteConnectionError
                | client_exc.ClusterError
                | client_exc.ProtocolError
                | OSError,
            ):
                return True
            code = getattr(cause, "code", None)
            if isinstance(cause, dbapi_exc.OperationalError | client_exc.OperationalError):
                if code in LEADER_ERROR_CODES or isinstance(cause, dbapi_exc.AmbiguousCommitError):
                    return True
                text = (getattr(cause, "raw_message", None) or str(cause)).lower()
                if code == 1 and any(marker in text for marker in _RAFT_DISCONNECT_MARKERS):
                    return True
                if code is None and any(m in text for m in _OPERATIONAL_DISCONNECT_MARKERS):
                    return True
            elif isinstance(cause, dbapi_exc.InterfaceError):
                text = (getattr(cause, "raw_message", None) or str(cause)).lower()
                if any(marker in text for marker in _INTERFACE_DISCONNECT_MARKERS):
                    return True
            elif type(cause) is dbapi_exc.DatabaseError and code in BARE_DATABASE_ERROR_CODES:
                return True
        return False

    def do_close(self, dbapi_connection: Any) -> None:
        try:
            dbapi_connection.close()
        except (dbapi_exc.Error, client_exc.DqliteError, OSError, RuntimeError):
            logger.debug("do_close: graceful close failed; dropping the transport", exc_info=True)
            dbapi_connection.force_close_transport()

    def do_terminate(self, dbapi_connection: Any) -> None:
        dbapi_connection.force_close_transport()


# -- DDL kwarg guard ---------------------------------------------------------------------


def _reject_dqlite_ddl_prefix(target: Any, parent: Any) -> None:
    """``dqlite_*`` DDL kwargs would be silently ignored (the SQLite DDL compiler reads
    ``sqlite_*``); refuse them at construction with the right spelling."""
    options = target.dialect_options.get("dqlite")
    passed = getattr(options, "_non_defaults", None) if options is not None else None
    if passed:
        names = sorted(passed)
        raise ArgumentError(
            f"{type(target).__name__} received dqlite_* DDL kwarg(s) "
            f"{[f'dqlite_{n}' for n in names]}; use {[f'sqlite_{n}' for n in names]} instead"
        )


for _construct in (sa_schema.Table, sa_schema.Index, sa_schema.Column, sa_schema.Constraint):
    event.listen(_construct, "after_parent_attach", _reject_dqlite_ddl_prefix)
