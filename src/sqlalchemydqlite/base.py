"""Base dqlite dialect for SQLAlchemy."""

import contextlib
import datetime
import warnings
from typing import Any

from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel
from sqlalchemy.exc import ArgumentError

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc


class _DqliteDateTime(sqltypes.DateTime):
    """Passthrough DateTime — ``dqlitedbapi`` already returns ``datetime.datetime``
    for columns declared as DATETIME/TIMESTAMP (matching PEP 249 and the
    psycopg/mysqlclient convention), so no string parsing is needed.
    """

    def bind_processor(self, dialect: Any) -> None:
        return None

    def result_processor(self, dialect: Any, coltype: Any) -> None:
        return None


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

    def result_processor(self, dialect: Any, coltype: Any) -> Any:
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

    # dqlite's wire protocol has a first-class BOOLEAN tag
    # (``ValueType.BOOLEAN = 11``); the server returns native Python
    # booleans for columns tagged BOOLEAN and dqlitedbapi passes them
    # through unchanged. Unlike the inherited pysqlite dialect
    # (``supports_native_boolean = False``), we don't need SQLAlchemy
    # to emit a ``CHECK (col IN (0, 1))`` constraint — the wire
    # contract enforces the 0/1 invariant.
    supports_native_boolean = True

    # dqlite runs every statement through Raft consensus; there is no
    # exposed way to weaken isolation. Declaring this explicitly lets
    # applications introspect via ``engine.dialect.supported_isolation_levels``.
    supported_isolation_levels: tuple[str, ...] = ("SERIALIZABLE",)

    # Override the SQLite dialect's string-based DATE/DATETIME processors:
    # dqlitedbapi returns datetime objects (PEP 249), not ISO strings.
    colspecs = {
        **SQLiteDialect.colspecs,
        sqltypes.DateTime: _DqliteDateTime,
        sqltypes.Date: _DqliteDate,
    }

    @classmethod
    def import_dbapi(cls) -> Any:
        import dqlitedbapi

        return dqlitedbapi

    # Whitelist of URL query parameters we forward to the DBAPI connect
    # call. Unknown keys raise ``ArgumentError`` so typos surface.
    _URL_QUERY_ALLOWED: dict[str, type] = {"timeout": float, "max_total_rows": int}

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from URL.

        URL format: ``dqlite://host:port/database?timeout=...``

        Known query parameters are typed and passed through to the
        DBAPI. Unknown ones raise :class:`ArgumentError` so typos like
        ``?timeoutt=5`` don't silently disappear.
        """
        host = url.host or "localhost"
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
            converter = self._URL_QUERY_ALLOWED[key]
            try:
                kwargs[key] = converter(raw)
            except (TypeError, ValueError) as e:
                raise ArgumentError(
                    f"Cannot convert URL query {key}={raw!r} to {converter.__name__}: {e}"
                ) from e

        return [], kwargs

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

    # Server-side SQLite error codes that mean the connection is useless
    # even if the TCP socket is still alive — kept in sync with the
    # client's _LEADER_ERROR_CODES.
    #   SQLITE_IOERR_NOT_LEADER       = SQLITE_IOERR | (40 << 8) = 10250
    #   SQLITE_IOERR_LEADERSHIP_LOST  = SQLITE_IOERR | (41 << 8) = 10506
    _LEADER_CHANGE_CODES: frozenset[int] = frozenset({10250, 10506})

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
        # Underlying OS-level transport failures (socket RST, broken pipe,
        # DNS, connect refused) surface as these stdlib types.
        if isinstance(e, (ConnectionError, BrokenPipeError, TimeoutError, OSError)):
            return True
        # Leader-change error codes signal that the connection is useless
        # even though it's TCP-alive.
        for err in (_dbapi_exc.OperationalError, _client_exc.OperationalError):
            if isinstance(e, err) and getattr(e, "code", None) in self._LEADER_CHANGE_CODES:
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
                TimeoutError,
            ):
                return False
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # cursor close shouldn't crash the ping result

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return the server version as a tuple.

        dqlite uses SQLite internally, so we return SQLite version. The
        parser is defensive: SQLite occasionally ships with suffixes
        (``3.46.0-alpha``, ``3.46.0rc1``) and a non-numeric part would
        otherwise raise ``ValueError`` on ``int()`` — we strip any
        trailing non-digit characters per component so the numeric
        prefix still parses.
        """
        import re

        try:
            result = connection.exec_driver_sql("SELECT sqlite_version()")
            version_str = result.scalar()
            if version_str:
                parts: list[int] = []
                for component in version_str.split("."):
                    m = re.match(r"\d+", component)
                    if not m:
                        break
                    parts.append(int(m.group(0)))
                if parts:
                    return tuple(parts)
        except (
            _dbapi_exc.OperationalError,
            _client_exc.DqliteConnectionError,
        ):
            pass
        return (3, 0, 0)


# Register the dialect
dialect = DqliteDialect
