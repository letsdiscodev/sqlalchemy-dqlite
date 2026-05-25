"""SQLAlchemy test suite requirements for dqlite dialect."""

# The ``exclusions.open()`` / ``closed()`` helpers are genuinely untyped
# at their source in ``sqlalchemy.testing.exclusions``, so mypy reports
# ``no-untyped-call`` (for the invocation) and ``no-any-return`` (for
# the return, since the helper's inferred type is ``Any``) on every
# single call below if we don't silence them. Narrowing each property's
# return annotation to ``compound`` (the actual runtime type) is still
# worthwhile — it gives IDE/hover signal and catches typos where a
# property returns a non-``compound`` object — but the two call-site
# silences have to stay until SA adds annotations upstream.
# mypy: disable-error-code="no-untyped-call, no-any-return"

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.exclusions import compound
from sqlalchemy.testing.requirements import SuiteRequirements

__all__ = ["Requirements"]


class Requirements(SuiteRequirements):
    """Test suite requirements for dqlite dialect.

    Override requirements that dqlite doesn't support.
    """

    @property
    def datetime_literals(self) -> compound:
        """SA's SQLite ``_DateTimeMixin.literal_processor`` emits
        ISO8601 string literals (e.g. ``'2012-10-15 12:57:18'``).
        dqlite stores them as TEXT and decodes via the same ISO8601
        path used for parameterised binds — no extra failure surface.

        The previous closure rationale ("SQLite doesn't have native
        datetime literals") conflated "no native datetime-typed
        literal syntax" with "the dialect can't render a literal at
        all". SA's literal_processor side-steps that exactly by
        rendering as a string. Verified against the live cluster
        for ``DateTime``, ``Date``, and ``Time``."""
        return exclusions.open()

    @property
    def time_microseconds(self) -> compound:
        """The dbapi's ``_iso8601_from_time`` emits microsecond
        precision via ``.{microsecond:06d}`` and the result decoder
        round-trips through ``time.fromisoformat`` which accepts µs
        precision, so the dialect preserves microseconds end-to-end."""
        return exclusions.open()

    @property
    def datetime_historic(self) -> compound:
        """SA's default SQLite DATETIME storage format is TEXT/ISO8601
        (``%Y-%m-%d %H:%M:%S.%f``), which has no lower bound. Verified
        against the live cluster: ``datetime(1850, 11, 10, 11, 52, 35)``
        round-trips losslessly.

        The base ``SuiteRequirements`` default (``closed()``) is
        conservative for backends that store datetimes as int ticks
        (Unix-time integer with a 1970 floor); SQLite text storage
        doesn't share that limit. The previous closure rationale
        ("SQLite date range limitation") was wrong — it described a
        constraint that doesn't apply to SA's default SQLite storage
        format."""
        return exclusions.open()

    @property
    def date_historic(self) -> compound:
        """Same rationale as :attr:`datetime_historic` — SA's SQLite
        DATE storage is TEXT/ISO8601, no lower bound. Verified
        against the live cluster: ``date(1727, 6, 11)`` round-trips
        losslessly. SA's base ``SuiteRequirements`` default is
        ``closed()`` (conservative for int-tick storage backends);
        opening here is the symmetric fix to the
        ``datetime_historic`` opening above."""
        return exclusions.open()

    @property
    def datetime_timezone(self) -> compound:
        """Target dialect supports timezone-aware ``datetime`` values.

        ``_DqliteDateTime.result_processor`` honours the type's
        ``timezone`` flag and re-attaches UTC for naive cells / converts
        through UTC for non-UTC inputs (see ``base.py``). ISO8601 TEXT
        storage round-trips the offset.

        SA's ``SuiteRequirements`` default is ``closed()``; opening
        here exercises the compliance suite's tz battery (e.g.
        ``DateTimeTZTest``) against the dialect's tz round-trip
        instead of skipping silently."""
        return exclusions.open()

    @property
    def time_timezone(self) -> compound:
        """Target dialect supports timezone-aware ``time`` values.

        Symmetric with :attr:`datetime_timezone` — ``_DqliteTime``
        handles tz-aware times via the same ISO8601 path. SA's base
        default is ``closed()``; opening here unlocks the compliance
        suite's ``TimeTZTest`` cases."""
        return exclusions.open()

    @property
    def unicode_ddl(self) -> compound:
        """SQLite supports unicode in DDL."""
        return exclusions.open()

    @property
    def savepoints(self) -> compound:
        """dqlite supports savepoints."""
        return exclusions.open()

    @property
    def two_phase_transactions(self) -> compound:
        """dqlite doesn't support two-phase transactions."""
        return exclusions.closed()

    @property
    def skip_autocommit_rollback(self) -> compound:
        """The dialect implements ``detect_autocommit_setting`` (returns
        False unconditionally; see :meth:`DqliteDialect.detect_autocommit_setting`)
        and inherits SA's default ``do_rollback``. Per SA's marker
        docstring ("target dialect supports the
        detect_autocommit_setting() method and uses the default
        implementation of do_rollback()"), this enables the SA
        compliance suite tests for the autocommit-rollback fast-path
        optimisation. Without the explicit ``open()``, those tests are
        skipped silently (SA default is ``exclusions.closed()``), so a
        regression in ``detect_autocommit_setting`` would not surface
        from compliance runs.
        """
        return exclusions.open()

    @property
    def temp_table_reflection(self) -> compound:
        """SA's ``_default_multi_reflect`` (engine/default.py) routes
        ``ObjectScope.DEFAULT`` through ``get_table_names``
        (``sqlite_master``) and ``ObjectScope.TEMPORARY`` through
        ``get_temp_table_names`` (``sqlite_temp_master``), keeping
        the two scopes segregated. The pysqlite reflection methods
        we inherit from ``SQLiteDialect_pysqlite`` implement both
        correctly and dqlite serves both relations identically to
        SQLite — verified at two levels:

        * SQL-surface: ``sqlite_temp_master`` and
          ``PRAGMA temp.table_info`` work end-to-end against the
          cluster.
        * Scoping (load-bearing for xdist): TEMP TABLE creates are
          per-connection, NOT shared across wire connections — see
          ``tests/integration/test_temp_table_scoping_per_connection.py``
          for the live cross-connection isolation pin. Without that
          scoping, parallel xdist workers would corrupt each
          other's ``sqlite_temp_master`` reflection.

        The full ``ComponentReflectionTest`` battery — single-table
        temp reflection plus the temp-scope ``test_get_multi_*``
        parametrize variants — passes when this is opened together
        with ``temp_table_names`` and ``has_temp_table`` (see
        overrides below)."""
        return exclusions.open()

    # --- Baseline declarations mirroring SQLite behavior ------------
    # These don't change the effective default (SuiteRequirements already
    # returns exclusions.open() for most), but make the dqlite contract
    # explicit so a future maintainer running the SQLAlchemy compliance
    # suite has a single source of truth to adjust.

    @property
    def ctes(self) -> compound:
        """Common Table Expressions (WITH)."""
        return exclusions.open()

    @property
    def window_functions(self) -> compound:
        """SQL window functions (OVER / PARTITION BY)."""
        return exclusions.open()

    @property
    def insert_returning(self) -> compound:
        """RETURNING clause on INSERT."""
        return exclusions.open()

    @property
    def update_returning(self) -> compound:
        """RETURNING clause on UPDATE."""
        return exclusions.open()

    @property
    def delete_returning(self) -> compound:
        """RETURNING clause on DELETE."""
        return exclusions.open()

    @property
    def insert_from_select(self) -> compound:
        """INSERT INTO ... SELECT."""
        return exclusions.open()

    @property
    def on_update_cascade(self) -> compound:
        """ON UPDATE CASCADE foreign-key action."""
        return exclusions.open()

    @property
    def self_referential_foreign_keys(self) -> compound:
        """Table references itself via foreign key."""
        return exclusions.open()

    @property
    def unique_constraint_reflection(self) -> compound:
        """Inspector reports UNIQUE constraints."""
        return exclusions.open()

    @property
    def primary_key_constraint_reflection(self) -> compound:
        """Inspector reports PRIMARY KEY constraints."""
        return exclusions.open()

    @property
    def foreign_key_constraint_reflection(self) -> compound:
        """Inspector reports FOREIGN KEY constraints."""
        return exclusions.open()

    @property
    def index_reflection(self) -> compound:
        """Inspector reports indexes."""
        return exclusions.open()

    @property
    def temporary_tables(self) -> compound:
        """CREATE TEMPORARY TABLE support (same as SQLite)."""
        return exclusions.open()

    @property
    def temp_table_names(self) -> compound:
        """SQLite (and dqlite) supports listing temporary table
        names via ``sqlite_temp_master``. SA's base default is
        ``closed()``; opening here is required so the suite's
        ``test_get_multi_*`` tests under ``ObjectScope.TEMPORARY``
        receive the temp-table dimension in their expected
        results (see ``test_reflection.py::_resolve_names``)."""
        return exclusions.open()

    @property
    def has_temp_table(self) -> compound:
        """SQLite (and dqlite) supports checking a single temp
        table via ``PRAGMA temp.table_info(...)``. SA's base
        default is ``closed()``; we have the capability so we
        open it for parity with the rest of the temp-table
        reflection contract."""
        return exclusions.open()

    @property
    def table_ddl_if_exists(self) -> compound:
        """CREATE TABLE IF NOT EXISTS / DROP TABLE IF EXISTS."""
        return exclusions.open()

    @property
    def sane_rowcount(self) -> compound:
        """UPDATE / DELETE rowcount is truthful. dqlite forwards the server's
        sqlite3_changes() verbatim via ResultResponse.rows_affected."""
        return exclusions.open()

    @property
    def sane_multi_rowcount(self) -> compound:
        """executemany aggregates each iteration's rowcount, so multi-row
        UPDATE / DELETE totals match the caller's expectation."""
        return exclusions.open()

    @property
    def emulated_lastrowid(self) -> compound:
        """lastrowid is SQLite's ROWID, forwarded verbatim via
        ResultResponse.last_insert_id."""
        return exclusions.open()

    @property
    def empty_inserts(self) -> compound:
        """INSERT INTO t DEFAULT VALUES. SQLite supports it; dqlite inherits."""
        return exclusions.open()

    @property
    def independent_connections(self) -> compound:
        """dqlite's Raft consensus gives every connection an independent
        transactional view of the database — concurrent connections do
        not share cache state, so the compliance suite's two-connection
        visibility checks pass unchanged."""
        return exclusions.open()

    @property
    def reflects_pk_names(self) -> compound:
        """SQLite (and therefore dqlite) reflects explicitly-named PK
        constraints — ``CONSTRAINT email_ad_pk PRIMARY KEY (...)`` is
        round-trippable through ``insp.get_pk_constraint``. The base
        ``SuiteRequirements`` default is ``closed()`` (most RDBMS do
        not preserve PK names through reflection); override here so
        the suite's ``fail_if(reflects_pk_names)`` block correctly
        asserts the name comes back rather than xfailing."""
        return exclusions.open()

    @property
    def parens_in_union_contained_select_w_limit_offset(self) -> compound:
        """SQLite (and therefore dqlite) rejects parenthesised
        SELECTs inside UNION when LIMIT/OFFSET is present:
        ``(SELECT ... LIMIT N) UNION (SELECT ... LIMIT N)`` raises
        ``near "UNION": syntax error``. The base requirement is
        ``open()``; close it for dqlite parity with SQLite's
        documented limitation."""
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self) -> compound:
        """SQLite (and therefore dqlite) rejects parenthesised
        SELECTs inside UNION when LIMIT/OFFSET is NOT present:
        ``(SELECT ...) UNION (SELECT ...)`` raises
        ``near "UNION": syntax error``. The base requirement is
        ``open()``; close it for dqlite parity with SQLite's
        documented limitation."""
        return exclusions.closed()

    @property
    def implicitly_named_constraints(self) -> compound:
        """SQLite (and therefore dqlite) does NOT implicitly name
        UNIQUE / CHECK / FK constraints — ``PRAGMA foreign_key_list()``
        and ``PRAGMA index_list()`` return ``None`` / empty strings
        for the constraint name when the DDL omitted it. The base
        ``SuiteRequirements`` default is ``open()`` (most RDBMS DO
        synthesise names); override here so the suite's
        ``fail_if(implicitly_named_constraints)`` blocks correctly
        xfail the assertion that names are non-None on auto-named
        SQLite constraints."""
        return exclusions.closed()

    @property
    def schemas(self) -> compound:
        """SQLite supports schema-qualified table names via
        ``ATTACH DATABASE <file> AS <schema>``; dqlite does NOT.

        The pysqlite reference hook attaches a sidecar SQLite file
        as the ``test_schema`` schema via a ``connect``-event listener.
        dqlite has no equivalent: the cluster-side ``ATTACH`` would
        need to bind a second cluster database, but the protocol
        opens one database per ``DqliteConnection`` — there is no
        runtime path to materialise a second database object on the
        same socket. The compliance suite's schema-qualified DDL
        (e.g. ``CREATE TABLE test_schema.users (...)``) therefore
        fails with ``unknown database test_schema``.

        Mark schemas closed until either:
        1. dqlite gains a multi-database-per-connection primitive, or
        2. The provision module grows a sidecar ATTACH path that
           opens an in-process SQLite file (loses cluster semantics
           but unlocks the suite's schema tests).

        Closing skips ~700 schema-qualified compliance tests; the
        ones that exercise pure-default-schema behaviour continue
        to run."""
        return exclusions.closed()

    @property
    def views(self) -> compound:
        """CREATE VIEW is supported by SQLite (and therefore dqlite)."""
        return exclusions.open()

    @property
    def autoincrement_insert(self) -> compound:
        """``INTEGER PRIMARY KEY AUTOINCREMENT`` is supported."""
        return exclusions.open()

    @property
    def standalone_binds(self) -> compound:
        """Bind parameters without a surrounding statement
        (e.g. ``SELECT :value``) are supported."""
        return exclusions.open()

    @property
    def order_by_label_with_expression(self) -> compound:
        """``ORDER BY <label>`` where the label refers back to an
        expression in the SELECT list — SQLite allows this; dqlite
        inherits. Re-pin locally so an upstream SQLiteDialect flip
        cannot silently skip these compliance cases."""
        return exclusions.open()

    @property
    def cross_schema_fk_reflection(self) -> compound:
        """SQLite's FK reflection is single-schema only — ``ATTACH``ed
        schemas do not surface cross-schema FK relationships through
        ``PRAGMA foreign_key_list``. dqlite inherits the limitation."""
        return exclusions.closed()

    @property
    def regexp_match(self) -> compound:
        """The portable ``col.regexp_match(pattern)`` operator compiles
        to ``col REGEXP ?``, which SQLite dispatches to a user-defined
        ``regexp`` function. pysqlite registers that function via
        ``dbapi_connection.create_function`` on every new connection
        (see ``SQLiteDialect_pysqlite.on_connect``); dqlite is a network
        DBAPI and has no ``create_function`` hook — registering a
        server-side function would require persisting into Raft state
        across all nodes, which is not part of the dqlite protocol.

        Running the compliance suite's ``regexp_match`` cases against
        dqlite would therefore hit ``OperationalError: no such function:
        regexp``. Close the requirement so the suite skips those cases
        instead of failing.
        """
        return exclusions.closed()

    @property
    def insert_executemany_returning(self) -> compound:
        """INSERT ... RETURNING with executemany() — dqlite supports
        it via the dialect pin ``insert_executemany_returning = True``
        (see ``base.py``) and the ``test_bulk_dml_returning.py``
        integration test. SA's default returns
        ``only_if(dialect.insert_executemany_returning)`` — so this
        is already effective-open via the dialect flag, but the
        explicit declaration is drift-defense (mirrors the pattern
        used by ``insert_returning`` / ``update_returning`` /
        ``delete_returning`` above).
        """
        return exclusions.open()

    @property
    def empty_inserts_executemany(self) -> compound:
        """``INSERT DEFAULT VALUES`` with executemany(). SA's default
        returns ``self.empty_inserts``, which we already declare as
        open above. Pinning this explicitly makes the contract
        explicit at the executemany site for future maintainers."""
        return exclusions.open()

    @property
    def ctes_with_update_delete(self) -> compound:
        """CTEs riding a normal UPDATE / DELETE. SQLite >= 3.35 (the
        minimum dqlite ships) supports ``WITH ... UPDATE`` and
        ``WITH ... DELETE``. SA's base default is ``closed()`` —
        explicitly opening this is the only behaviour-changing
        declaration in this set."""
        return exclusions.open()

    @property
    def foreign_key_ddl(self) -> compound:
        """FOREIGN KEY DDL phrases. SQLite supports the syntax;
        dqlite inherits. The existing
        ``foreign_key_constraint_reflection`` declaration implies
        this; pin it explicitly for grep parity."""
        return exclusions.open()

    @property
    def named_constraints(self) -> compound:
        """Named ``UNIQUE`` / ``CHECK`` / ``PRIMARY KEY`` /
        ``FOREIGN KEY`` constraints in DDL. SQLite-native; dqlite
        inherits."""
        return exclusions.open()

    @property
    def unicode_connections(self) -> compound:
        """Non-ASCII bind / result data. dqlite's wire protocol is
        UTF-8 end to end; the dbapi cursors return native ``str`` for
        TEXT columns. The existing ``unicode_ddl`` declaration
        implies the connection-level support; pin it explicitly."""
        return exclusions.open()

    @property
    def graceful_disconnects(self) -> compound:
        """``InterfaceError`` on closed-connection ``execute()`` — the
        dbapi (sync and async) raises per PEP 249, and the SA adapter
        layer honours that. Existing tests pin every branch of
        ``_check_in_use`` and the closed-state PEP 249 contracts."""
        return exclusions.open()

    # SuiteRequirements properties below default to ``closed()`` upstream;
    # opening them adds compliance-suite coverage for SQL features that
    # SQLite (and therefore dqlite) supports natively.

    @property
    def nullsordering(self) -> compound:
        """``ORDER BY ... NULLS FIRST/LAST``. SQLite 3.30+ supports
        natively (dqlite-server runs SQLite 3.35)."""
        return exclusions.open()

    @property
    def intersect(self) -> compound:
        """``SELECT ... INTERSECT SELECT ...`` set operator. SQLite
        3.0+ supports."""
        return exclusions.open()

    @property
    def except_(self) -> compound:
        """``SELECT ... EXCEPT SELECT ...`` set operator. SQLite
        3.0+ supports."""
        return exclusions.open()

    @property
    def index_ddl_if_exists(self) -> compound:
        """``CREATE INDEX IF NOT EXISTS`` / ``DROP INDEX IF EXISTS``.
        SQLite 3.3+ supports."""
        return exclusions.open()
