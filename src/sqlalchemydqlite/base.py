"""Base dqlite dialect for SQLAlchemy."""

from typing import Any

from sqlalchemy import pool
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel


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

    # Default to NullPool since dqlite handles connection pooling internally
    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return pool.NullPool

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

        dqlite doesn't support changing isolation levels via PRAGMA,
        so this is a no-op. dqlite uses SERIALIZABLE isolation by default.
        """
        pass

    def do_rollback(self, dbapi_connection: DBAPIConnection) -> None:
        """Rollback the current transaction.

        dqlite throws an error if we try to rollback when no transaction
        is active, so we catch and ignore that specific error.
        """
        try:
            dbapi_connection.rollback()
        except Exception as e:
            # Ignore "no transaction is active" errors
            if "no transaction is active" not in str(e):
                raise

    def do_commit(self, dbapi_connection: DBAPIConnection) -> None:
        """Commit the current transaction.

        dqlite throws an error if we try to commit when no transaction
        is active, so we catch and ignore that specific error.
        """
        try:
            dbapi_connection.commit()
        except Exception as e:
            # Ignore "no transaction is active" errors
            if "no transaction is active" not in str(e):
                raise

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
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False

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
