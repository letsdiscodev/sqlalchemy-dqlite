"""Tests for dqlite dialect."""

from sqlalchemy.engine import URL

from sqlalchemydqlite import DqliteDialect
from sqlalchemydqlite.aio import DqliteDialect_aio


class TestDqliteDialect:
    def test_dialect_name(self) -> None:
        dialect = DqliteDialect()
        assert dialect.name == "dqlite"

    def test_dialect_driver(self) -> None:
        dialect = DqliteDialect()
        assert dialect.driver == "dqlitedbapi"

    def test_paramstyle(self) -> None:
        dialect = DqliteDialect()
        assert dialect.paramstyle == "qmark"

    def test_import_dbapi(self) -> None:
        dbapi = DqliteDialect.import_dbapi()
        assert hasattr(dbapi, "connect")
        assert hasattr(dbapi, "apilevel")
        assert dbapi.apilevel == "2.0"

    def test_create_connect_args_default(self) -> None:
        dialect = DqliteDialect()
        url = URL.create("dqlite")

        args, kwargs = dialect.create_connect_args(url)

        assert args == []
        assert kwargs["address"] == "localhost:9001"
        assert kwargs["database"] == "default"

    def test_create_connect_args_custom(self) -> None:
        dialect = DqliteDialect()
        url = URL.create("dqlite", host="node1", port=9002, database="mydb")

        args, kwargs = dialect.create_connect_args(url)

        assert kwargs["address"] == "node1:9002"
        assert kwargs["database"] == "mydb"


class TestDqliteDialectAio:
    def test_dialect_name(self) -> None:
        dialect = DqliteDialect_aio()
        assert dialect.name == "dqlite"

    def test_dialect_is_async(self) -> None:
        dialect = DqliteDialect_aio()
        assert dialect.is_async is True

    def test_import_dbapi(self) -> None:
        dbapi = DqliteDialect_aio.import_dbapi()
        assert hasattr(dbapi, "aconnect")

    def test_import_dbapi_has_paramstyle(self) -> None:
        """Async dbapi module must expose paramstyle for SQLAlchemy dialect init."""
        dbapi = DqliteDialect_aio.import_dbapi()
        assert dbapi.paramstyle == "qmark"

    def test_import_dbapi_has_module_attributes(self) -> None:
        """Async dbapi module must expose PEP 249 attributes for SQLAlchemy."""
        dbapi = DqliteDialect_aio.import_dbapi()
        assert dbapi.apilevel == "2.0"
        assert dbapi.threadsafety == 1

    def test_create_async_engine(self) -> None:
        """create_async_engine must not raise during dialect initialization."""
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine("dqlite+aio://localhost:19001/test")
        assert engine.dialect.name == "dqlite"
        assert engine.dialect.driver == "dqlitedbapi_aio"


class TestURLParsing:
    def test_parse_basic_url(self) -> None:
        url = URL.create("dqlite", host="localhost", port=9001, database="test")
        assert url.host == "localhost"
        assert url.port == 9001
        assert url.database == "test"

    def test_url_string_format(self) -> None:
        url = URL.create("dqlite", host="node1", port=9001, database="mydb")
        assert str(url) == "dqlite://node1:9001/mydb"

    def test_aio_url_string_format(self) -> None:
        url = URL.create("dqlite+aio", host="node1", port=9001, database="mydb")
        assert str(url) == "dqlite+aio://node1:9001/mydb"
