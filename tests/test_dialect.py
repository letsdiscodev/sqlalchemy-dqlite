"""Tests for dqlite dialect."""

import pytest
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

    def test_supports_sane_isolation_level(self) -> None:
        # dqlite always enforces SERIALIZABLE; the reported level is
        # trustworthy across transactions.
        assert DqliteDialect.supports_sane_isolation_level is True

    def test_supports_native_decimal_false(self) -> None:
        # SQLite/dqlite has no native DECIMAL type.
        assert DqliteDialect.supports_native_decimal is False

    def test_returning_flags_pinned_locally(self) -> None:
        # dqlite runs SQLite >= 3.35, so RETURNING is supported. Pin the
        # three SQLAlchemy 2.x flags locally so upstream dialect changes
        # cannot silently alter dqlite behaviour.
        assert DqliteDialect.insert_returning is True
        assert DqliteDialect.update_returning is True
        assert DqliteDialect.delete_returning is True
        # Locally declared (not just inherited) so the pin is load-bearing.
        assert "insert_returning" in DqliteDialect.__dict__
        assert "update_returning" in DqliteDialect.__dict__
        assert "delete_returning" in DqliteDialect.__dict__

    def test_supports_multivalues_insert_pinned_locally(self) -> None:
        # SQLite >= 3.7.11 supports multi-row INSERT VALUES; dqlite does too.
        assert DqliteDialect.supports_multivalues_insert is True
        assert "supports_multivalues_insert" in DqliteDialect.__dict__

    def test_non_native_boolean_check_constraint_pinned_locally(self) -> None:
        # dqlite declares supports_native_boolean = True, so the CHECK
        # constraint is semantically unnecessary. Pin to False so a
        # future SQLAlchemy release cannot silently flip the inherited
        # value while native boolean support is already claimed.
        assert DqliteDialect.non_native_boolean_check_constraint is False
        assert "non_native_boolean_check_constraint" in DqliteDialect.__dict__

    def test_update_returning_multifrom_pinned_locally(self) -> None:
        # dqlite's SQLite is >= 3.35, which supports multi-FROM RETURNING.
        # Pin locally for the same upstream-drift reason as the three
        # RETURNING flags above.
        assert DqliteDialect.update_returning_multifrom is True
        assert "update_returning_multifrom" in DqliteDialect.__dict__

    @pytest.mark.parametrize(
        "flag",
        [
            "use_insertmanyvalues",
            "supports_default_metavalue",
            "supports_default_values",
            "insert_null_pk_still_autoincrements",
        ],
    )
    def test_insert_path_flags_pinned_locally(self, flag: str) -> None:
        """SQLAlchemy's SQLiteDialect sets these four insert-path flags
        explicitly. The dqlite dialect inherits them silently otherwise.
        Pin locally so upstream version-gated changes cannot alter
        insert codegen or rowid behaviour for dqlite.
        """
        assert getattr(DqliteDialect, flag) is True
        assert flag in DqliteDialect.__dict__

    def test_supports_server_side_cursors_pinned_false_on_aio(self) -> None:
        """dqlite has no server-side cursor notion; pin locally on the
        async dialect so an upstream AsyncDialect default flip cannot
        silently route through an SS-cursor code path we do not
        implement.
        """
        from sqlalchemydqlite.aio import DqliteDialect_aio

        assert DqliteDialect_aio.supports_server_side_cursors is False
        assert "supports_server_side_cursors" in DqliteDialect_aio.__dict__

    def test_returns_native_bytes_pinned_locally(self) -> None:
        """dqlitedbapi returns native Python ``bytes`` for BLOB columns;
        pin True locally so ``LargeBinary.result_processor`` skips the
        redundant ``bytes(value)`` wrap on every BLOB cell, and so a
        future DefaultDialect default flip cannot silently add overhead.
        """
        assert DqliteDialect.returns_native_bytes is True
        assert "returns_native_bytes" in DqliteDialect.__dict__

    def test_dialect_description(self) -> None:
        # Pin the derived dialect_description so SQLAlchemy upgrades cannot
        # silently change the rendered identity in ORM error messages.
        assert DqliteDialect().dialect_description == "dqlite+dqlitedbapi"

    def test_async_dialect_description(self) -> None:
        # Mirror the sync test for the async dialect; review agent flagged
        # that the sync test alone could mask an async-side drift.
        assert DqliteDialect_aio().dialect_description == "dqlite+dqlitedbapi_aio"


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

    def test_returns_fallback_on_operational_error(self) -> None:
        """Should return (3, 0, 0) if the server query fails with a
        connection-level error. Unrelated errors (bugs) propagate."""
        from unittest.mock import MagicMock

        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_conn.exec_driver_sql.side_effect = dqlitedbapi.exceptions.OperationalError(
            "connection broken"
        )

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


class TestDoRollbackCommit:
    """The 'no transaction is active' swallow moved down to the DBAPI
    layer (python-dqlite-dbapi). The dialect now inherits do_commit /
    do_rollback from the parent and delegates straight to the DBAPI,
    which handles the no-active-tx case. These tests verify the dialect
    doesn't wrap the DBAPI's errors when a real problem occurs.
    """

    def test_do_rollback_propagates_real_errors(self) -> None:
        """Real (not 'no transaction') errors propagate through the dialect."""
        from unittest.mock import MagicMock

        import pytest

        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_conn.rollback.side_effect = dqlitedbapi.exceptions.OperationalError(
            "database is locked"
        )
        with pytest.raises(dqlitedbapi.exceptions.OperationalError, match="database is locked"):
            dialect.do_rollback(mock_conn)


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


class TestIsolationLevel:
    def test_set_isolation_level_warns_on_unsupported(self) -> None:
        """set_isolation_level should warn when a non-SERIALIZABLE level is requested."""
        import warnings
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dialect.set_isolation_level(mock_conn, "READ UNCOMMITTED")

        assert len(w) == 1
        assert "SERIALIZABLE" in str(w[0].message)
        assert "READ UNCOMMITTED" in str(w[0].message)

    def test_set_isolation_level_silent_for_serializable(self) -> None:
        """set_isolation_level should not warn for SERIALIZABLE."""
        import warnings
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dialect.set_isolation_level(mock_conn, "SERIALIZABLE")

        assert len(w) == 0

    def test_set_isolation_level_silent_for_none(self) -> None:
        """set_isolation_level should not warn when level is None."""
        import warnings
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dialect.set_isolation_level(mock_conn, None)

        assert len(w) == 0


class TestDoPing:
    def test_cursor_closed_in_finally(self) -> None:
        """do_ping must close cursor in a finally block."""
        import ast
        import inspect
        import textwrap

        source = textwrap.dedent(inspect.getsource(DqliteDialect.do_ping))
        tree = ast.parse(source)

        has_finally_close = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Try) and node.finalbody:
                for stmt in ast.walk(node):
                    if isinstance(stmt, ast.Call):
                        func_node = stmt.func
                        if isinstance(func_node, ast.Attribute) and func_node.attr == "close":
                            has_finally_close = True

        assert has_finally_close, "cursor.close() should be in a finally block in do_ping"

    def test_ping_returns_true_on_success(self) -> None:
        """do_ping should return True when query succeeds."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        assert dialect.do_ping(mock_conn) is True

    def test_ping_returns_false_on_connection_error(self) -> None:
        """do_ping returns False on connection-level errors."""
        from unittest.mock import MagicMock

        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.execute.side_effect = dqlitedbapi.exceptions.OperationalError(
            "bye"
        )
        assert dialect.do_ping(mock_conn) is False

    def test_ping_closes_cursor_even_on_error(self) -> None:
        """do_ping must close cursor even when execute fails with a
        connection-level error."""
        from unittest.mock import MagicMock

        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = dqlitedbapi.exceptions.OperationalError("bye")
        mock_conn.cursor.return_value = mock_cursor

        dialect.do_ping(mock_conn)
        mock_cursor.close.assert_called_once()


class TestPoolClass:
    def test_sync_dialect_does_not_use_nullpool(self) -> None:
        """Sync dialect should not default to NullPool for a network database."""
        from sqlalchemy import pool
        from sqlalchemy.engine import URL

        url = URL.create("dqlite", host="localhost", port=9001, database="test")
        pool_class = DqliteDialect.get_pool_class(url)
        assert pool_class is not pool.NullPool, (
            "NullPool creates a new TCP connection per operation; "
            "a network database should use QueuePool"
        )


class TestAsyncConnect:
    def test_connect_calls_await_only_on_raw_connect(self) -> None:
        """Async dialect connect() should eagerly establish the TCP connection."""
        import ast
        import inspect
        import textwrap

        from sqlalchemydqlite.aio import DqliteDialect_aio

        source = textwrap.dedent(inspect.getsource(DqliteDialect_aio.connect))
        tree = ast.parse(source)

        # Look for await_only(raw_conn.connect()) or similar eager connect call
        has_eager_connect = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "await_only" and node.args:
                    arg = node.args[0]
                    if isinstance(arg, ast.Call):
                        inner = arg.func
                        if isinstance(inner, ast.Attribute) and inner.attr == "connect":
                            has_eager_connect = True

        assert has_eager_connect, (
            "DqliteDialect_aio.connect() should eagerly establish TCP with "
            "await_only(raw_conn.connect())"
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
