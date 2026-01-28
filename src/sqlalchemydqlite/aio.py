"""Async dqlite dialect for SQLAlchemy."""

from typing import Any

from sqlalchemy import pool
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool


class DqliteDialect_aio(SQLiteDialect):  # noqa: N801
    """Async SQLAlchemy dialect for dqlite.

    Use with SQLAlchemy's async engine:
        create_async_engine("dqlite+aio://host:port/database")
    """

    name = "dqlite"
    driver = "dqlitedbapi_aio"
    is_async = True

    # dqlite uses qmark parameter style
    paramstyle = "qmark"

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(cls) -> Any:
        from dqlitedbapi import aio

        return aio

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Create connection arguments from URL.

        URL format: dqlite+aio://host:port/database
        """
        host = url.host or "localhost"
        port = url.port or 9001
        database = url.database or "default"

        address = f"{host}:{port}"

        return [], {
            "address": address,
            "database": database,
        }

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the driver-level connection."""
        return connection


# Register the dialect
dialect = DqliteDialect_aio
