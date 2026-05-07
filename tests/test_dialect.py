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

    def test_update_returning_multifrom_inherited_true(self) -> None:
        # dqlite's SQLite is >= 3.35, which supports multi-FROM RETURNING.
        # Inherited from ``SQLiteDialect`` (class body, not version
        # gate); the trio above (insert/update/delete_returning) is
        # version-gated in ``SQLiteDialect.__init__`` and re-pinned
        # there, but multifrom is not part of that gate. Pinning
        # locally would defend against a class-body change in
        # ``SQLiteDialect`` itself — narrower than the trio's drift
        # surface and not previously load-bearing.
        assert DqliteDialect.update_returning_multifrom is True

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
    def test_insert_path_flags_true(self, flag: str) -> None:
        """The four insert-path flags read True on the dialect.

        ``use_insertmanyvalues`` and ``insert_null_pk_still_autoincrements``
        are pinned locally as drift defence against a hypothetical
        ``DefaultDialect`` default change. ``supports_default_metavalue``
        is now inherited from ``SQLiteDialect``'s class body.
        ``supports_default_values`` has both a class-attr pin (pre-init
        baseline) and an ``__init__`` re-pin against the parent's
        version-gated reset.
        """
        assert getattr(DqliteDialect, flag) is True

    def test_default_metavalue_token_inherited_null(self) -> None:
        """``default_metavalue_token`` is the SQL token emitted on an
        autoincrement-rowid PK column for ``insertmanyvalues``. The
        ``DefaultDialect`` uses ``"DEFAULT"`` (rejected by SQLite); the
        ``SQLiteDialect`` parent overrides to ``"NULL"``. Inherited
        through the pysqlite parent — no local pin needed. The
        previous version of this test asserted ``__dict__`` membership
        as drift defence against a ``DefaultDialect`` flip; under the
        pysqlite parent the SQLite-level override is stable.
        """
        assert DqliteDialect.default_metavalue_token == "NULL"

    def test_tuple_in_values_inherited_true(self) -> None:
        """``tuple_in_values`` drives row-value ``IN`` clause
        rendering. SQLite supports the syntax; the parent
        ``SQLiteDialect`` sets True in its class body. Inherited;
        no local pin needed."""
        assert DqliteDialect.tuple_in_values is True

    def test_alter_and_empty_insert_inherited_false(self) -> None:
        """``supports_alter`` and ``supports_empty_insert`` are
        non-default overrides on the SQLite parent: SQLite's
        ``ALTER TABLE`` is limited and ``INSERT () VALUES ()`` is a
        syntax error. Inherited from ``SQLiteDialect``'s class body
        through the pysqlite parent."""
        assert DqliteDialect.supports_alter is False
        assert DqliteDialect.supports_empty_insert is False

    def test_supports_server_side_cursors_pinned_false_on_aio(self) -> None:
        """dqlite has no server-side cursor notion; pin locally on the
        async dialect so an upstream AsyncDialect default flip cannot
        silently route through an SS-cursor code path we do not
        implement.
        """
        from sqlalchemydqlite.aio import DqliteDialect_aio

        assert DqliteDialect_aio.supports_server_side_cursors is False
        assert "supports_server_side_cursors" in DqliteDialect_aio.__dict__

    def test_supports_server_side_cursors_pinned_false_on_sync(self) -> None:
        """Same defensive pin on the sync dialect. The DefaultDialect
        chain currently sets False, but a future upstream change could
        flip the inherited default; pin locally so the contract is
        anchored at the dqlite dialect class itself.
        """
        assert DqliteDialect.supports_server_side_cursors is False
        assert "supports_server_side_cursors" in DqliteDialect.__dict__

    def test_returns_native_bytes_inherited_true(self) -> None:
        """Pin ``returns_native_bytes is True`` so SA's
        ``LargeBinary.result_processor`` skips the redundant
        ``bytes(value)`` wrap on every BLOB cell.

        Inherited from ``SQLiteDialect_pysqlite``, which sets True
        explicitly in its class body. The previous version of this
        test asserted ``__dict__`` membership against a feared
        ``DefaultDialect`` default flip; under the pysqlite parent
        that drift surface no longer exists (pysqlite's value sits
        between us and ``DefaultDialect``, and SA would not flip
        pysqlite's value to False without a major performance
        regression for every pysqlite user). Drop the
        ``__dict__`` assertion; pin only the effective value.
        """
        assert DqliteDialect.returns_native_bytes is True

    def test_description_encoding_pinned_locally_to_none(self) -> None:
        """Drift defence: ``description_encoding`` is pinned on
        ``DqliteDialect`` itself.

        dqlitedbapi returns ``str`` column names (matches pysqlite); a
        non-``None`` value here would route descriptions through SA's
        byte-decode pipeline and crash with ``AttributeError`` on
        every column-name access. Pysqlite already pins ``None``;
        the local re-pin is documentary parity with the rest of this
        block — guards against any future upstream refactor that
        moves the field out from under us.
        """
        assert "description_encoding" in DqliteDialect.__dict__
        assert DqliteDialect.description_encoding is None

    def test_isolation_lookup_pinned_locally_to_serializable_only(self) -> None:
        """Drift defence: ``_isolation_lookup`` advertises only the
        level the dialect actually accepts.

        Pysqlite's inherited lookup includes ``READ UNCOMMITTED`` and
        ``AUTOCOMMIT`` keys we reject at runtime
        (``set_isolation_level``). The pinned single-level mapping is
        the truthful surface for SA-internal paths that read the
        lookup by-key. Note: deliberately diverges from
        ``get_isolation_level_values`` which advertises AUTOCOMMIT
        as a diagnostic-routing channel — see the comment block in
        ``base.py`` for why the two surfaces differ.
        """
        assert "_isolation_lookup" in DqliteDialect.__dict__
        assert dict(DqliteDialect._isolation_lookup) == {"SERIALIZABLE": 0}

    def test_dialect_description(self) -> None:
        # Pin the derived dialect_description so SQLAlchemy upgrades cannot
        # silently change the rendered identity in ORM error messages.
        # The sync convention is the dbapi-module-name (matches SA's
        # pysqlite reference); the async sibling uses URL-shape parity
        # (driver = "aio") because ``dqlite+aio://`` IS what the user
        # types. The sync URL ``dqlite://`` has no ``+driver`` suffix
        # so the URL-parity argument doesn't apply here. See the
        # ``driver`` block comment in ``base.py`` for the full
        # rationale and the SA precedent.
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

    def test_explicit_dqlitedbapi_driver_url_resolves(self) -> None:
        """SA URL ``dqlite+dqlitedbapi://`` resolves to the same dialect
        class as the bare ``dqlite://`` form.

        Pysqlite (built into SA) makes ``sqlite+pysqlite://`` resolvable
        via SA's internal ``_auto_fn`` name-splitting; we are external,
        so we must register both bare and explicit-driver entry points
        in ``pyproject.toml``. URL-canonical templating that emits the
        ``<dialect>+<driver>://`` form is the common third-party shape,
        so the explicit form must round-trip.
        """
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
        """Async dialect should inherit shared methods from base, not duplicate them."""
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

    def test_async_dialect_do_begin_emits_begin_over_wire(self) -> None:
        """do_begin must emit an explicit BEGIN over the wire. The
        inherited SQLiteDialect_pysqlite.do_begin is a no-op because
        pysqlite auto-emits BEGIN before the first DML via the
        connection-level ``isolation_level`` attribute. The dqlite
        dbapi has no equivalent mechanism — without an explicit BEGIN
        every INSERT inside ``engine.begin()`` auto-commits at the
        server, defeating transactional atomicity. Pin the corrected
        contract: do_begin opens a cursor, executes ``BEGIN``, closes
        the cursor, and does not touch the connection-level
        commit / rollback / close paths."""
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
        inner = _client_exc.OperationalError("not the leader", SQLITE_IOERR_NOT_LEADER)
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
