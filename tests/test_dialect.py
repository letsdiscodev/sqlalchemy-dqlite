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

    def test_supports_native_decimal_false(self) -> None:
        # SQLite/dqlite has no native DECIMAL type.
        assert DqliteDialect.supports_native_decimal is False

    def test_returning_flags_pinned_locally(self) -> None:
        # dqlite runs SQLite >= 3.35; pin the three flags locally so upstream
        # dialect changes cannot silently alter dqlite behaviour.
        assert DqliteDialect.insert_returning is True
        assert DqliteDialect.update_returning is True
        assert DqliteDialect.delete_returning is True
        assert "insert_returning" in DqliteDialect.__dict__
        assert "update_returning" in DqliteDialect.__dict__
        assert "delete_returning" in DqliteDialect.__dict__

    def test_supports_multivalues_insert_pinned_locally(self) -> None:
        assert DqliteDialect.supports_multivalues_insert is True
        assert "supports_multivalues_insert" in DqliteDialect.__dict__

    def test_non_native_boolean_check_constraint_pinned_locally(self) -> None:
        # dqlite claims supports_native_boolean, so the CHECK constraint is
        # unnecessary; pin False so an upstream flip can't re-add it.
        assert DqliteDialect.non_native_boolean_check_constraint is False
        assert "non_native_boolean_check_constraint" in DqliteDialect.__dict__

    def test_update_returning_multifrom_inherited_true(self) -> None:
        # Inherited from SQLiteDialect's class body (not the version gate that
        # covers the insert/update/delete_returning trio).
        assert DqliteDialect.update_returning_multifrom is True

    def test_executemany_returning_flags_pinned_locally(self) -> None:
        # INSERT is a DefaultDialect memoized property; UPDATE/DELETE default
        # False (which blocks executemany RETURNING). Pin all three True.
        assert DqliteDialect.insert_executemany_returning is True
        assert DqliteDialect.update_executemany_returning is True
        assert DqliteDialect.delete_executemany_returning is True
        assert "insert_executemany_returning" in DqliteDialect.__dict__
        assert "update_executemany_returning" in DqliteDialect.__dict__
        assert "delete_executemany_returning" in DqliteDialect.__dict__

    @pytest.mark.parametrize(
        "flag",
        [
            "use_insertmanyvalues",
            "supports_default_metavalue",
            "supports_default_values",
            "insert_null_pk_still_autoincrements",
        ],
    )
    def test_insert_path_flags_true(self, flag: str) -> None:
        """The four insert-path flags read True on the dialect."""
        assert getattr(DqliteDialect, flag) is True

    def test_default_metavalue_token_inherited_null(self) -> None:
        """SQLiteDialect overrides default_metavalue_token to "NULL" (DefaultDialect's
        "DEFAULT" is rejected by SQLite); inherited, no local pin needed."""
        assert DqliteDialect.default_metavalue_token == "NULL"

    def test_tuple_in_values_inherited_true(self) -> None:
        """tuple_in_values (row-value IN rendering) inherited True from SQLiteDialect."""
        assert DqliteDialect.tuple_in_values is True

    def test_alter_and_empty_insert_inherited_false(self) -> None:
        """supports_alter / supports_empty_insert inherited False (SQLite's ALTER is
        limited and INSERT () VALUES () is a syntax error)."""
        assert DqliteDialect.supports_alter is False
        assert DqliteDialect.supports_empty_insert is False

    def test_supports_server_side_cursors_pinned_false_on_aio(self) -> None:
        """dqlite has no server-side cursor notion; pin False locally on the async dialect."""
        from sqlalchemydqlite.aio import DqliteDialect_aio

        assert DqliteDialect_aio.supports_server_side_cursors is False
        assert "supports_server_side_cursors" in DqliteDialect_aio.__dict__

    def test_supports_server_side_cursors_pinned_false_on_sync(self) -> None:
        """Same defensive pin on the sync dialect."""
        assert DqliteDialect.supports_server_side_cursors is False
        assert "supports_server_side_cursors" in DqliteDialect.__dict__

    def test_returns_native_bytes_inherited_true(self) -> None:
        """returns_native_bytes True (inherited from pysqlite) lets LargeBinary skip
        a redundant bytes(value) wrap on every BLOB cell."""
        assert DqliteDialect.returns_native_bytes is True

    def test_description_encoding_pinned_locally_to_none(self) -> None:
        """description_encoding pinned None: dqlitedbapi returns str column names, so a
        non-None value would route through SA's byte-decode path and crash."""
        assert "description_encoding" in DqliteDialect.__dict__
        assert DqliteDialect.description_encoding is None

    def test_isolation_lookup_pinned_locally_to_serializable_only(self) -> None:
        """_isolation_lookup advertises only SERIALIZABLE (pysqlite's includes
        READ UNCOMMITTED / AUTOCOMMIT keys we reject at runtime). Deliberately
        diverges from get_isolation_level_values, which routes AUTOCOMMIT for diagnostics."""
        assert "_isolation_lookup" in DqliteDialect.__dict__
        assert dict(DqliteDialect._isolation_lookup) == {"SERIALIZABLE": 0}

    def test_dialect_description(self) -> None:
        # Sync uses the dbapi-module-name convention (SA's pysqlite precedent);
        # see the driver block comment in base.py.
        assert DqliteDialect().dialect_description == "dqlite+dqlitedbapi"

    def test_async_dialect_description(self) -> None:
        # Async uses driver="aio" — the URL shape the user actually types.
        assert DqliteDialect_aio().dialect_description == "dqlite+aio"

    def test_async_driver_matches_entry_point(self) -> None:
        """driver must equal the entry-point's second component: EP "dqlite.aio"
        means users type dqlite+aio://, so driver must be "aio"."""
        import importlib.metadata as md

        eps = md.entry_points(group="sqlalchemy.dialects")
        ep_map = {ep.name: ep for ep in eps}
        aio_ep = ep_map["dqlite.aio"]
        assert aio_ep.name.split(".", 1)[1] == DqliteDialect_aio.driver

    def test_explicit_dqlitedbapi_driver_url_resolves(self) -> None:
        """dqlite+dqlitedbapi:// resolves to the same class as bare dqlite://.
        Being external, we must register both entry points in pyproject.toml
        (SA's _auto_fn name-splitting only covers built-in dialects like pysqlite)."""
        from sqlalchemy.engine import make_url

        bare = make_url("dqlite://localhost:9001/db").get_dialect()
        explicit = make_url("dqlite+dqlitedbapi://localhost:9001/db").get_dialect()

        assert bare is explicit, f"Expected the same dialect class, got {bare} vs {explicit}"


class TestDqliteDialectAio:
    def test_dialect_name(self) -> None:
        dialect = DqliteDialect_aio()
        assert dialect.name == "dqlite"

    def test_dialect_is_async(self) -> None:
        dialect = DqliteDialect_aio()
        assert dialect.is_async is True

    def test_inherits_shared_methods_from_base(self) -> None:
        shared_methods = [
            "create_connect_args",
            "do_begin",
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
        dbapi = DqliteDialect_aio.import_dbapi()
        assert dbapi.paramstyle == "qmark"

    def test_import_dbapi_has_module_attributes(self) -> None:
        dbapi = DqliteDialect_aio.import_dbapi()
        assert dbapi.apilevel == "2.0"
        # threadsafety=2: threads may share module and connections (not cursors);
        # see dqlitedbapi.__init__.
        assert dbapi.threadsafety == 2

    def test_create_async_engine(self) -> None:
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine("dqlite+aio://localhost:19001/test")
        assert engine.dialect.name == "dqlite"
        assert engine.dialect.driver == "aio"


class TestGetServerVersionInfo:
    def test_reads_dbapi_module_constant(self) -> None:
        """Forwards dqlitedbapi.sqlite_version_info — no live query."""
        from unittest.mock import MagicMock

        import dqlitedbapi

        dialect = DqliteDialect()
        dialect.dbapi = dqlitedbapi  # type: ignore[assignment]
        mock_conn = MagicMock()

        result = dialect._get_server_version_info(mock_conn)
        assert result == tuple(dqlitedbapi.sqlite_version_info)
        mock_conn.exec_driver_sql.assert_not_called()

    def test_does_not_downgrade_on_transient_error(self) -> None:
        """Reads the DBAPI module constant directly — no error path yields a stale tuple."""
        from unittest.mock import MagicMock

        import dqlitedbapi

        dialect = DqliteDialect()
        dialect.dbapi = dqlitedbapi  # type: ignore[assignment]
        mock_conn = MagicMock()
        mock_conn.exec_driver_sql.side_effect = RuntimeError("should not be called")

        result = dialect._get_server_version_info(mock_conn)
        assert result == tuple(dqlitedbapi.sqlite_version_info)

    def test_respects_dbapi_version_attribute(self) -> None:
        """A bump in the DBAPI's pinned SQLite version flows through without further wiring."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        dialect.dbapi = SimpleNamespace(sqlite_version_info=(3, 46, 0))  # type: ignore[assignment]

        result = dialect._get_server_version_info(MagicMock())
        assert result == (3, 46, 0)

    def test_propagates_attribute_error_when_dbapi_lacks_attribute(self) -> None:
        """A DBAPI lacking sqlite_version_info must raise AttributeError, not silently
        engage RETURNING / multi-values against a driver that may not implement them."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        import pytest

        dialect = DqliteDialect()
        dialect.dbapi = SimpleNamespace()  # type: ignore[assignment]

        with pytest.raises(AttributeError):
            dialect._get_server_version_info(MagicMock())


class TestGetDriverConnection:
    def test_async_dialect_returns_underlying_connection(self) -> None:
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
    """do_commit/do_rollback are inherited and delegate to the DBAPI (which swallows
    no-active-tx); these tests verify the dialect doesn't wrap real DBAPI errors."""

    def test_do_rollback_propagates_real_errors(self) -> None:
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

    def test_async_dialect_do_begin_emits_begin_over_wire(self) -> None:
        """do_begin must emit an explicit BEGIN over the wire (the inherited pysqlite
        no-op relies on auto-BEGIN that the dqlite dbapi lacks); without it every
        INSERT inside engine.begin() auto-commits at the server."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        dialect.do_begin(mock_conn)

        mock_conn.cursor.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with("BEGIN")
        mock_cursor.close.assert_called_once_with()
        # do_begin must NOT touch the connection-level methods.
        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_not_called()
        mock_conn.close.assert_not_called()

    def test_async_dialect_do_commit_delegates_to_connection(self) -> None:
        """do_commit must call dbapi_conn.commit(); pin the call shape so a future SA
        refactor that awaits do_commit (double-awaiting our greenlet bridge) breaks loudly."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        dialect.do_commit(mock_conn)
        mock_conn.commit.assert_called_once_with()

    def test_async_dialect_do_rollback_delegates_to_connection(self) -> None:
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        dialect.do_rollback(mock_conn)
        mock_conn.rollback.assert_called_once_with()


class TestIsDisconnect:
    def test_recognizes_connection_closed(self) -> None:
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Connection closed by server")
        assert dialect.is_disconnect(e, None, None) is True

    def test_recognizes_failed_to_connect(self) -> None:
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Failed to connect: refused")
        assert dialect.is_disconnect(e, None, None) is True

    def test_does_not_flag_normal_errors(self) -> None:
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("no such table: users")
        assert dialect.is_disconnect(e, None, None) is False

    def test_recognizes_not_connected(self) -> None:
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Not connected to database")
        assert dialect.is_disconnect(e, None, None) is True

    def test_recognizes_timed_out(self) -> None:
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.OperationalError("Connection timed out")
        assert dialect.is_disconnect(e, None, None) is True

    def test_is_defined_on_dialect(self) -> None:
        assert "is_disconnect" in DqliteDialect.__dict__, (
            "DqliteDialect must override is_disconnect"
        )

    def test_recognizes_interface_error_connection_closed(self) -> None:
        """A post-close InterfaceError must classify as disconnect so the pool recycles."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.InterfaceError("Connection is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_recognizes_interface_error_cursor_closed(self) -> None:
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.InterfaceError("Cursor is closed")
        assert dialect.is_disconnect(e, None, None) is True

    def test_does_not_flag_other_interface_errors(self) -> None:
        """Programming-error InterfaceErrors (e.g. bad arraysize) must NOT be disconnects."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.InterfaceError("arraysize must be positive")
        assert dialect.is_disconnect(e, None, None) is False

    @pytest.mark.parametrize(
        "exc",
        [
            OSError(32, "broken pipe"),
            BrokenPipeError(32, "broken pipe"),
            ConnectionError("peer went away"),
            ConnectionResetError(104, "connection reset by peer"),
            TimeoutError("read timed out"),
        ],
    )
    def test_recognizes_os_level_disconnect_branches(self, exc: BaseException) -> None:
        """Pin each transport-failure branch (OSError, ConnectionError, BrokenPipeError,
        TimeoutError) so narrowing the is_disconnect tuple fails loudly."""
        dialect = DqliteDialect()
        assert dialect.is_disconnect(exc, None, None) is True

    def test_does_not_flag_programming_error(self) -> None:
        """ProgrammingError is not a transport failure and must not be a disconnect."""
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.ProgrammingError("no such function: foo")
        assert dialect.is_disconnect(e, None, None) is False

    def test_recognizes_wrapped_dqlite_connection_error_via_cause(self) -> None:
        """A DqliteConnectionError wrapped into a bare OperationalError is only caught
        by walking __cause__; pin the chain inspection."""
        import dqliteclient.exceptions as _client_exc
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        original = _client_exc.DqliteConnectionError("peer RST")
        try:
            raise dqlitedbapi.exceptions.OperationalError("wrapped") from original
        except dqlitedbapi.exceptions.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_recognizes_wrapped_leader_change_via_cause(self) -> None:
        """A re-wrapped leader-change OperationalError must still classify as disconnect
        (requires walking the cause chain)."""
        import dqliteclient.exceptions as _client_exc
        import dqlitedbapi.exceptions
        from dqlitewire.constants import SQLITE_IOERR_NOT_LEADER

        dialect = DqliteDialect()
        inner = _client_exc.OperationalError("not the leader", SQLITE_IOERR_NOT_LEADER)
        try:
            raise dqlitedbapi.exceptions.OperationalError("wrapped") from inner
        except dqlitedbapi.exceptions.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True


class TestIsolationLevel:
    def test_set_isolation_level_raises_on_unsupported(self) -> None:
        """A non-SERIALIZABLE level raises ArgumentError rather than silently coercing
        (which would change the caller's requested semantics)."""
        from unittest.mock import MagicMock

        import pytest
        from sqlalchemy.exc import ArgumentError

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        with pytest.raises(ArgumentError, match="only supports SERIALIZABLE"):
            dialect.set_isolation_level(mock_conn, "READ UNCOMMITTED")

    def test_set_isolation_level_silent_for_serializable(self) -> None:
        import warnings
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dialect.set_isolation_level(mock_conn, "SERIALIZABLE")

        assert len(w) == 0

    def test_set_isolation_level_none_rejected(self) -> None:
        """None is rejected with ArgumentError (the reset path no longer routes here)."""
        from unittest.mock import MagicMock

        import pytest
        from sqlalchemy.exc import ArgumentError

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        with pytest.raises(ArgumentError, match="non-None"):
            dialect.set_isolation_level(mock_conn, None)  # type: ignore[arg-type]
        mock_conn.cursor.assert_not_called()

    def test_reset_isolation_level_silent_for_serializable_sync(self) -> None:
        """Pin the pool-return reset chain for a SERIALIZABLE-configured engine:
        reset_isolation_level resolves to set_isolation_level("SERIALIZABLE"), a no-op."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        dialect._on_connect_isolation_level = "SERIALIZABLE"
        dialect.default_isolation_level = "SERIALIZABLE"
        mock_conn = MagicMock()

        dialect.reset_isolation_level(mock_conn)
        mock_conn.cursor.assert_not_called()

    def test_reset_isolation_level_silent_for_serializable_aio(self) -> None:
        """Async-side mirror: the inherited reset_isolation_level chain stays silent."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        dialect._on_connect_isolation_level = "SERIALIZABLE"
        dialect.default_isolation_level = "SERIALIZABLE"
        mock_conn = MagicMock()

        dialect.reset_isolation_level(mock_conn)
        mock_conn.cursor.assert_not_called()


class TestDoPing:
    def test_cursor_closed_in_finally(self) -> None:
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
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        assert dialect.do_ping(mock_conn) is True

    def test_ping_returns_false_on_connection_error(self) -> None:
        from unittest.mock import MagicMock

        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.execute.side_effect = dqlitedbapi.exceptions.OperationalError(
            "bye"
        )
        assert dialect.do_ping(mock_conn) is False

    def test_ping_closes_cursor_even_on_error(self) -> None:
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
        import ast
        import inspect
        import textwrap

        from sqlalchemydqlite.aio import DqliteDialect_aio

        source = textwrap.dedent(inspect.getsource(DqliteDialect_aio.connect))
        tree = ast.parse(source)

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
