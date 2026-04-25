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

    def test_executemany_returning_flags_pinned_locally(self) -> None:
        # dqlitedbapi's executemany accumulates per-parameter-set
        # RETURNING rows via _ExecuteManyAccumulator. All three DML kinds
        # deliver the full row set in one call. Integration-verified in
        # tests/integration/test_bulk_dml_returning.py.
        #
        # INSERT: DefaultDialect exposes this as a memoized property
        # (derived from ``insert_returning and use_insertmanyvalues``) —
        # pinning locally ensures upstream drift can't silently flip it.
        # UPDATE / DELETE: DefaultDialect defaults False, which blocks
        # SQLAlchemy from issuing executemany RETURNING; pin True to
        # surface the capability.
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
        # Must match the entry-point short name so the rendered form is
        # the URL the user actually types (``dqlite+aio://``).
        assert DqliteDialect_aio().dialect_description == "dqlite+aio"

    def test_async_driver_matches_entry_point(self) -> None:
        """Pin ``Dialect.driver`` == EP's second component.

        SA convention: ``dialect_description`` renders
        ``"{name}+{driver}"`` using the exact ``+driver`` suffix a user
        writes into the URL. The entry-point name ``"dqlite.aio"``
        means users type ``dqlite+aio://``, so ``driver`` must be
        ``"aio"`` — any drift here renders a non-canonical description
        string and breaks log-grep of the URL shape.
        """
        import importlib.metadata as md

        eps = md.entry_points(group="sqlalchemy.dialects")
        ep_map = {ep.name: ep for ep in eps}
        aio_ep = ep_map["dqlite.aio"]
        assert aio_ep.name.split(".", 1)[1] == DqliteDialect_aio.driver


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
        # Critically, no wire round-trip.
        mock_conn.exec_driver_sql.assert_not_called()

    def test_does_not_downgrade_on_transient_error(self) -> None:
        """The previous fallback to (3, 0, 0) silently disabled 3.35+
        features on any transient error. The new implementation uses
        the DBAPI module constant directly, so there is no error path
        that could produce a stale tuple.
        """
        from unittest.mock import MagicMock

        import dqlitedbapi

        dialect = DqliteDialect()
        dialect.dbapi = dqlitedbapi  # type: ignore[assignment]
        mock_conn = MagicMock()
        mock_conn.exec_driver_sql.side_effect = RuntimeError("should not be called")

        result = dialect._get_server_version_info(mock_conn)
        assert result == tuple(dqlitedbapi.sqlite_version_info)

    def test_respects_dbapi_version_attribute(self) -> None:
        """A hypothetical bump in the DBAPI's pinned SQLite version
        should flow through to the dialect without further wiring.
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        dialect.dbapi = SimpleNamespace(sqlite_version_info=(3, 46, 0))  # type: ignore[assignment]

        result = dialect._get_server_version_info(MagicMock())
        assert result == (3, 46, 0)

    def test_propagates_attribute_error_when_dbapi_lacks_attribute(self) -> None:
        """A broken / stubbed DBAPI module that does not expose
        ``sqlite_version_info`` must surface as ``AttributeError`` at
        dialect-init — not silently engage RETURNING / multi-values
        against a driver that may not implement them. Matches the
        one-liner upstream pysqlite uses.
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        import pytest

        dialect = DqliteDialect()
        dialect.dbapi = SimpleNamespace()  # type: ignore[assignment]

        with pytest.raises(AttributeError):
            dialect._get_server_version_info(MagicMock())


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

    def test_async_dialect_do_begin_calls_dbapi_connection(self) -> None:
        """The async dialect inherits do_begin from DefaultDialect, but
        the inherited implementation is a pass-through. Pin that the
        async dialect does NOT override do_begin in a way that fails
        to call the dbapi connection — and that the inherited
        no-op contract still holds."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        # DefaultDialect.do_begin is a no-op (pass) — the dbapi's
        # implicit-BEGIN handling kicks in on the first DML. Pin the
        # contract: do_begin must not raise and must not perform any
        # spurious connection.commit / rollback / close.
        dialect.do_begin(mock_conn)
        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_not_called()
        mock_conn.close.assert_not_called()

    def test_async_dialect_do_commit_delegates_to_connection(self) -> None:
        """do_commit on the async dialect must call dbapi_conn.commit() —
        which on the AsyncAdaptedConnection routes through the
        await_only greenlet bridge to the underlying async client.
        Pin the call shape so a future SA refactor that begins
        awaiting do_commit on async dialects (turning our greenlet
        bridge into a double-await) breaks loudly."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        dialect.do_commit(mock_conn)
        mock_conn.commit.assert_called_once_with()

    def test_async_dialect_do_rollback_delegates_to_connection(self) -> None:
        """Mirror of do_commit; pin the rollback delegation shape."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        dialect.do_rollback(mock_conn)
        mock_conn.rollback.assert_called_once_with()


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

    def test_recognizes_interface_error_connection_closed(self) -> None:
        """An InterfaceError raised after the underlying DBAPI connection
        was closed (e.g. pool invalidate, cluster membership change)
        must be classified as disconnect so the pool recycles the slot.
        """
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
        """Narrowly-worded programming-error InterfaceErrors (e.g.
        `arraysize must be positive`) must NOT route through the
        disconnect path.
        """
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
        """The ``is_disconnect`` tuple includes OSError, ConnectionError,
        BrokenPipeError, and TimeoutError. ``ConnectionError`` and
        ``TimeoutError`` are semantically distinct from OSError on
        Python 3.11+ — pin each branch so a refactor narrowing the
        tuple (e.g. dropping ConnectionError because "OSError covers
        it on Linux") would fail loudly and not silently break
        pool invalidation on transport failures.
        """
        dialect = DqliteDialect()
        assert dialect.is_disconnect(exc, None, None) is True

    def test_does_not_flag_programming_error(self) -> None:
        """Inverse pin: ProgrammingError is not a transport failure
        and must not route through the disconnect path — otherwise SA
        would invalidate a healthy connection on e.g. a syntax error.
        """
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        e = dqlitedbapi.exceptions.ProgrammingError("no such function: foo")
        assert dialect.is_disconnect(e, None, None) is False

    def test_recognizes_wrapped_dqlite_connection_error_via_cause(self) -> None:
        """The dbapi ``_call_client`` handler wraps a client-level
        ``DqliteConnectionError`` into a bare ``OperationalError`` (no
        code). Without walking ``__cause__`` the direct
        isinstance branch would miss the wrapped form entirely. Pin
        the chain inspection so the dead-code isinstance branch above
        remains load-bearing for chained errors too.
        """
        import dqliteclient.exceptions as _client_exc
        import dqlitedbapi.exceptions

        dialect = DqliteDialect()
        original = _client_exc.DqliteConnectionError("peer RST")
        try:
            raise dqlitedbapi.exceptions.OperationalError("wrapped") from original
        except dqlitedbapi.exceptions.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_recognizes_wrapped_leader_change_via_cause(self) -> None:
        """A leader-change OperationalError that was re-wrapped one
        extra layer (by middleware, telemetry, the dbapi wrapper) must
        still classify as a disconnect. Without walking the cause
        chain the SA pool slot would stay alive while the connection
        is actually dead.
        """
        import dqliteclient.exceptions as _client_exc
        import dqlitedbapi.exceptions
        from dqlitewire.constants import SQLITE_IOERR_NOT_LEADER

        dialect = DqliteDialect()
        inner = _client_exc.OperationalError(SQLITE_IOERR_NOT_LEADER, "not the leader")
        try:
            raise dqlitedbapi.exceptions.OperationalError("wrapped") from inner
        except dqlitedbapi.exceptions.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True


class TestIsolationLevel:
    def test_set_isolation_level_raises_on_unsupported(self) -> None:
        """set_isolation_level must raise ArgumentError when a
        non-SERIALIZABLE level is requested. Silently coercing to
        SERIALIZABLE (the prior ``warnings.warn`` behaviour) would
        change the caller's requested semantics, the exact footgun
        the AUTOCOMMIT branch's ArgumentError was installed to
        prevent.
        """
        from unittest.mock import MagicMock

        import pytest
        from sqlalchemy.exc import ArgumentError

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        with pytest.raises(ArgumentError, match="only supports SERIALIZABLE"):
            dialect.set_isolation_level(mock_conn, "READ UNCOMMITTED")

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

    def test_set_isolation_level_none_does_not_touch_connection(self) -> None:
        """SA's pool resets isolation between checkouts via
        ``set_isolation_level(conn, None)``. Pin the true no-op contract:
        no cursor opened, no attribute accessed, no exception raised —
        a future refactor that routed ``None`` through autocommit
        setup would stay warning-free but still break SA's reset path,
        so observe at the mock-call level.
        """
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        mock_conn = MagicMock()

        dialect.set_isolation_level(mock_conn, None)

        # No cursor opened and no other attribute access on conn.
        mock_conn.cursor.assert_not_called()
        assert mock_conn.mock_calls == []

    def test_reset_isolation_level_silent_for_serializable_sync(self) -> None:
        """SA's ``DefaultDialect.reset_isolation_level`` fires on
        pool-return when ``_on_connect_isolation_level`` is set.
        For ``create_engine(..., isolation_level="SERIALIZABLE")`` the
        path is ``reset_isolation_level → _assert_and_set_isolation_level
        → set_isolation_level("SERIALIZABLE")``, which our dialect
        accepts silently. Pin the chain so a future SA refactor cannot
        silently break the reset path on a configured-isolation engine.
        """
        from unittest.mock import MagicMock

        dialect = DqliteDialect()
        dialect._on_connect_isolation_level = "SERIALIZABLE"
        dialect.default_isolation_level = "SERIALIZABLE"
        mock_conn = MagicMock()

        # Should be a clean no-op — no exception, no cursor opened.
        dialect.reset_isolation_level(mock_conn)
        mock_conn.cursor.assert_not_called()

    def test_reset_isolation_level_silent_for_serializable_aio(self) -> None:
        """Async-side mirror of the SERIALIZABLE pool-return reset path.
        The async dialect inherits ``reset_isolation_level``; pin that
        the inheritance chain remains intact and silent."""
        from unittest.mock import MagicMock

        dialect = DqliteDialect_aio()
        dialect._on_connect_isolation_level = "SERIALIZABLE"
        dialect.default_isolation_level = "SERIALIZABLE"
        mock_conn = MagicMock()

        dialect.reset_isolation_level(mock_conn)
        mock_conn.cursor.assert_not_called()


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
