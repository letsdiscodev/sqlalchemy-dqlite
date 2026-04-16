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

    def test_inherits_shared_methods_from_base(self) -> None:
        """Async dialect should inherit shared methods from base, not duplicate them."""
        shared_methods = [
            "create_connect_args",
            "do_rollback",
            "do_commit",
            "_get_server_version_info",
        ]
        for method_name in shared_methods:
            base_method = getattr(DqliteDialect, method_name)
            aio_method = getattr(DqliteDialect_aio, method_name)
            assert base_method is aio_method, (
                f"{method_name} is overridden in DqliteDialect_aio but should be inherited"
            )

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


class TestGetServerVersionInfo:
    def test_does_not_access_dbapi_connection_directly(self) -> None:
        """_get_server_version_info should use exec_driver_sql, not internal attributes."""
        import ast
        import inspect
        import textwrap

        source = textwrap.dedent(inspect.getsource(DqliteDialect._get_server_version_info))
        tree = ast.parse(source)

        # Check that it doesn't access .dbapi_connection
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "dbapi_connection":
                raise AssertionError(
                    "_get_server_version_info accesses .dbapi_connection directly; "
                    "should use connection.exec_driver_sql() instead"
                )

    def test_returns_fallback_on_error(self) -> None:
        """Should return (3, 0, 0) if the query fails."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_conn.exec_driver_sql.side_effect = Exception("connection broken")

        result = dialect._get_server_version_info(mock_conn)
        assert result == (3, 0, 0)

    def test_parses_version_string(self) -> None:
        """Should parse a version string like '3.39.4' into a tuple."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = "3.39.4"
        mock_conn.exec_driver_sql.return_value = mock_result

        result = dialect._get_server_version_info(mock_conn)
        assert result == (3, 39, 4)


class TestGetDriverConnection:
    def test_async_dialect_returns_underlying_connection(self) -> None:
        """get_driver_connection should return the raw connection, not the adapter."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_adapted = MagicMock()
        mock_adapted._connection = MagicMock(name="raw_async_connection")

        result = dialect.get_driver_connection(mock_adapted)
        assert result is mock_adapted._connection, (
            "get_driver_connection should unwrap to the underlying connection, "
            "not return the AsyncAdaptedConnection wrapper"
        )


class TestIsDisconnect:
    def test_recognizes_connection_closed(self) -> None:
        """is_disconnect should return True for connection-closed errors."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Connection closed by server")
        assert dialect.is_disconnect(e, None, None) is True

    def test_recognizes_failed_to_connect(self) -> None:
        """is_disconnect should return True for connection-failure errors."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Failed to connect: refused")
        assert dialect.is_disconnect(e, None, None) is True

    def test_does_not_flag_normal_errors(self) -> None:
        """is_disconnect should return False for normal operational errors."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("no such table: users")
        assert dialect.is_disconnect(e, None, None) is False

    def test_recognizes_not_connected(self) -> None:
        """is_disconnect should return True for 'not connected' errors."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Not connected to database")
        assert dialect.is_disconnect(e, None, None) is True

    def test_recognizes_timed_out(self) -> None:
        """is_disconnect should return True for timeout errors."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Connection timed out")
        assert dialect.is_disconnect(e, None, None) is True

    def test_is_defined_on_dialect(self) -> None:
        """DqliteDialect must define its own is_disconnect, not just inherit."""
        assert "is_disconnect" in DqliteDialect.__dict__, (
            "DqliteDialect must override is_disconnect"
        )


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
