"""Base dqlite dialect for SQLAlchemy."""

import datetime
from typing import Any

from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel


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
    """

    def bind_processor(self, dialect: Any) -> None:
        return None

    def result_processor(
        self, dialect: Any, coltype: Any
    ) -> Any:
        def process(value: Any) -> Any:
            if isinstance(value, datetime.datetime):
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

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from URL.

        URL format: dqlite://host:port/database
        """
        host = url.host or "localhost"
        port = url.port or 9001
        database = url.database or "default"

        address = f"{host}:{port}"

        return [], {
            "address": address,
            "database": database,
        }

    def get_isolation_level(self, dbapi_connection: DBAPIConnection) -> IsolationLevel:
        """Return the isolation level.

        dqlite doesn't support PRAGMA read_uncommitted, so we return
        SERIALIZABLE as the default isolation level.
        """
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection: DBAPIConnection, level: str | None) -> None:
        """Set isolation level.

        dqlite only supports SERIALIZABLE isolation. A warning is emitted
        if a different level is requested.
        """
        if level is not None and level != "SERIALIZABLE":
            import warnings

            warnings.warn(
                f"dqlite only supports SERIALIZABLE isolation. "
                f"Requested level {level!r} is ignored.",
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

        dqlite is a network database, so we must detect TCP-level and
        leader-change errors that the inherited pysqlite patterns miss.
        """
        import dqlitedbapi.exceptions

        if isinstance(e, dqlitedbapi.exceptions.OperationalError):
            msg = str(e)
            for pattern in self._dqlite_disconnect_messages:
                if pattern in msg:
                    return True
        return super().is_disconnect(e, connection, cursor)

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Check if the connection is still alive."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
            return True
        except Exception:
            return False
        finally:
            cursor.close()

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return the server version as a tuple.

        dqlite uses SQLite internally, so we return SQLite version.
        """
        try:
            result = connection.exec_driver_sql("SELECT sqlite_version()")
            version_str = result.scalar()
            if version_str:
                return tuple(int(x) for x in version_str.split("."))
        except Exception:
            pass
        return (3, 0, 0)


# Register the dialect
dialect = DqliteDialect
