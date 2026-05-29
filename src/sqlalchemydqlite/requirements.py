"""SQLAlchemy test suite requirements for dqlite dialect."""

# exclusions.open()/closed() are untyped upstream; silence until SA annotates them.
# mypy: disable-error-code="no-untyped-call, no-any-return"

from typing import Any

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.exclusions import compound
from sqlalchemy.testing.requirements import SuiteRequirements

__all__ = ["Requirements"]


class Requirements(SuiteRequirements):
    """Test suite requirements for dqlite dialect."""

    @property
    def datetime_literals(self) -> compound:
        """SA renders datetime literals as ISO8601 TEXT, which dqlite round-trips."""
        return exclusions.open()

    @property
    def time_microseconds(self) -> compound:
        """Microseconds round-trip end-to-end via ISO8601 encode/decode."""
        return exclusions.open()

    @property
    def datetime_historic(self) -> compound:
        """SQLite stores datetimes as ISO8601 TEXT (no lower bound); base default
        closed() targets int-tick backends with a 1970 floor, which doesn't apply."""
        return exclusions.open()

    @property
    def date_historic(self) -> compound:
        """Same as datetime_historic: SQLite DATE is ISO8601 TEXT, no lower bound."""
        return exclusions.open()

    @property
    def datetime_timezone(self) -> compound:
        """tz-aware datetimes: _DqliteDateTime honours the timezone flag and
        ISO8601 TEXT round-trips the offset."""
        return exclusions.open()

    @property
    def time_timezone(self) -> compound:
        """tz-aware times via the same ISO8601 path as datetime_timezone."""
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
        """Open so the suite's autocommit-rollback tests run; detect_autocommit_setting
        returns False, so the fast-path never fires through this dialect."""
        return exclusions.open()

    @property
    def temp_table_reflection(self) -> compound:
        """Temp-table reflection works; TEMP creates are per-connection (load-bearing
        for xdist worker isolation of sqlite_temp_master)."""
        return exclusions.open()

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
        """Listing temp table names via sqlite_temp_master."""
        return exclusions.open()

    @property
    def has_temp_table(self) -> compound:
        """Checking a single temp table via PRAGMA temp.table_info(...)."""
        return exclusions.open()

    @property
    def table_ddl_if_exists(self) -> compound:
        """CREATE TABLE IF NOT EXISTS / DROP TABLE IF EXISTS."""
        return exclusions.open()

    @property
    def sane_rowcount(self) -> compound:
        """Rowcount forwards the server's sqlite3_changes() via rows_affected."""
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
        """Raft gives each connection an independent transactional view (no shared cache)."""
        return exclusions.open()

    @property
    def reflects_pk_names(self) -> compound:
        """SQLite reflects explicitly-named PK constraints; base default is closed()."""
        return exclusions.open()

    @property
    def parens_in_union_contained_select_w_limit_offset(self) -> compound:
        """SQLite rejects parenthesised SELECTs in UNION with LIMIT/OFFSET."""
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self) -> compound:
        """SQLite rejects parenthesised SELECTs in UNION even without LIMIT/OFFSET."""
        return exclusions.closed()

    @property
    def implicitly_named_constraints(self) -> compound:
        """SQLite does NOT implicitly name UNIQUE/CHECK/FK constraints; base is open()."""
        return exclusions.closed()

    @property
    def schemas(self) -> compound:
        """No ATTACH DATABASE: the protocol opens one database per connection, so
        schema-qualified names cannot bind a second database."""
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
        """ORDER BY <label> referring to a SELECT-list expression."""
        return exclusions.open()

    def get_order_by_collation(self, config: Any) -> str:
        """Real collation so CollateTest runs (base raises NotImplementedError)."""
        return "NOCASE"

    @property
    def cross_schema_fk_reflection(self) -> compound:
        """SQLite FK reflection is single-schema only (PRAGMA foreign_key_list)."""
        return exclusions.closed()

    @property
    def regexp_match(self) -> compound:
        """REGEXP needs a UDF via create_function, which the dqlite network DBAPI
        has no hook for."""
        return exclusions.closed()

    @property
    def insert_executemany_returning(self) -> compound:
        """INSERT ... RETURNING with executemany()."""
        return exclusions.open()

    @property
    def empty_inserts_executemany(self) -> compound:
        """INSERT DEFAULT VALUES with executemany()."""
        return exclusions.open()

    @property
    def ctes_with_update_delete(self) -> compound:
        """WITH ... UPDATE/DELETE; SQLite >= 3.35 (dqlite minimum)."""
        return exclusions.open()

    @property
    def foreign_key_ddl(self) -> compound:
        """FOREIGN KEY DDL phrases."""
        return exclusions.open()

    @property
    def named_constraints(self) -> compound:
        """Named UNIQUE/CHECK/PRIMARY KEY/FOREIGN KEY constraints in DDL."""
        return exclusions.open()

    @property
    def unicode_connections(self) -> compound:
        """Non-ASCII bind/result data; the wire protocol is UTF-8 end to end."""
        return exclusions.open()

    @property
    def graceful_disconnects(self) -> compound:
        """InterfaceError on closed-connection execute() per PEP 249."""
        return exclusions.open()

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

    @property
    def json_type(self) -> compound:
        """Server-side SQLite has JSON1 enabled (default for SQLite >= 3.9)."""
        return exclusions.open()

    @property
    def json_array_indexes(self) -> compound:
        """JSON1 numeric array indexes via json_extract(col, '$[0]')."""
        return exclusions.open()

    @property
    def uuid_data_type(self) -> compound:
        """Uuid round-trips through BLOB storage; the wire codec preserves BLOB cells."""
        return exclusions.open()

    @property
    def computed_columns(self) -> compound:
        """GENERATED ALWAYS AS columns; SQLite >= 3.31."""
        return exclusions.open()

    @property
    def computed_columns_stored(self) -> compound:
        return exclusions.open()

    @property
    def computed_columns_virtual(self) -> compound:
        return exclusions.open()

    @property
    def computed_columns_default_persisted(self) -> compound:
        # GENERATED without STORED/VIRTUAL reflects as persisted=False on SQLite.
        return exclusions.closed()

    @property
    def computed_columns_reflect_persisted(self) -> compound:
        return exclusions.open()

    @property
    def autoincrement_without_sequence(self) -> compound:
        """INTEGER PRIMARY KEY AUTOINCREMENT; no separate sequence object."""
        return exclusions.open()

    @property
    def dbapi_lastrowid(self) -> compound:
        """Cursor.lastrowid survives cursor.close(), which SA's CursorResult relies on
        (it reads lastrowid lazily after close)."""
        return exclusions.open()

    @property
    def supports_lastrowid(self) -> compound:
        """See dbapi_lastrowid; lastrowid-based PK recovery works."""
        return exclusions.open()

    @property
    def identity_columns(self) -> compound:
        """SQLite has no GENERATED ... AS IDENTITY; uses INTEGER PRIMARY KEY
        AUTOINCREMENT instead."""
        return exclusions.closed()

    @property
    def check_constraint_reflection(self) -> compound:
        """CHECK constraints reflected from sqlite_master.sql."""
        return exclusions.open()

    @property
    def recursive_fk_cascade(self) -> compound:
        """ON DELETE CASCADE on a self-referential FK; SQLite >= 3.6.19."""
        return exclusions.open()

    @property
    def tuple_in(self) -> compound:
        """WHERE (x, y) IN ((1, 2), (3, 4)); SQLite >= 3.0."""
        return exclusions.open()

    @property
    def table_value_constructor(self) -> compound:
        """Closed: the suite's compiled VALUES+CTE shape isn't accepted everywhere,
        though the bare VALUES clause works in user SQL."""
        return exclusions.closed()

    @property
    def fetch_first(self) -> compound:
        """Closed: SA's compiler emits LIMIT/OFFSET for SQLite, so these tests
        never actually exercise FETCH FIRST."""
        return exclusions.closed()

    @property
    def server_defaults(self) -> compound:
        """column DEFAULT (expression) is server-evaluated."""
        return exclusions.open()

    @property
    def expression_server_defaults(self) -> compound:
        """column DEFAULT (3 * 5) — a server-evaluated expression default."""
        return exclusions.open()

    @property
    def float_or_double_precision_behaves_generically(self) -> compound:
        """SQLite stores every REAL as an 8-byte double, so Float(p)/Double(p)
        precision does not truncate."""
        return exclusions.open()

    @property
    def foreign_key_constraint_name_reflection(self) -> compound:
        """Explicit FK constraint name reflected from sqlite_master.sql."""
        return exclusions.open()

    @property
    def percent_schema_names(self) -> compound:
        """% and spaces in quoted identifiers (the name is misleading: not DB schemas)."""
        return exclusions.open()

    @property
    def comment_reflection(self) -> compound:
        """SQLite has no inline column comments to reflect."""
        return exclusions.closed()

    @property
    def indexes_with_expressions(self) -> compound:
        """Indexes on expressions, e.g. CREATE INDEX ix ON t(lower(col)); SQLite >= 3.9."""
        return exclusions.open()

    @property
    def reflect_tables_no_columns(self) -> compound:
        """SQLite's parser rejects CREATE TABLE t(); a zero-column table can't exist."""
        return exclusions.closed()

    @property
    def mod_operator_as_percent_sign(self) -> compound:
        """SQLite uses ``%`` as the modulus operator (not
        ``MOD(...)``)."""
        return exclusions.open()

    @property
    def regexp_replace(self) -> compound:
        """Needs a UDF via create_function, like regexp_match; dqlite has no hook."""
        return exclusions.closed()

    @property
    def supports_bitwise_or(self) -> compound:
        """SQLite ≥ 3.0 supports the ``|`` bitwise OR operator."""
        return exclusions.open()

    @property
    def supports_bitwise_and(self) -> compound:
        """SQLite ≥ 3.0 supports the ``&`` bitwise AND operator."""
        return exclusions.open()

    @property
    def supports_bitwise_not(self) -> compound:
        """SQLite ≥ 3.0 supports the ``~`` bitwise NOT operator."""
        return exclusions.open()

    @property
    def supports_bitwise_xor(self) -> compound:
        """No native XOR; SA compiles to the (a | b) & ~(a & b) fallback."""
        return exclusions.open()

    @property
    def supports_bitwise_shift(self) -> compound:
        """SQLite ≥ 3.0 supports ``<<`` and ``>>`` shift operators."""
        return exclusions.open()

    @property
    def infinity_floats(self) -> compound:
        """IEEE 754 infinities preserved via the wire codec's struct '<d' packing."""
        return exclusions.open()

    @property
    def ctes_with_values(self) -> compound:
        """WITH cte AS (VALUES ...); SQLite >= 3.8.3."""
        return exclusions.open()

    @property
    def update_from(self) -> compound:
        """UPDATE ... FROM; SQLite >= 3.33."""
        return exclusions.open()

    @property
    def foreign_key_constraint_option_reflection_ondelete(self) -> compound:
        """ON DELETE action reflected via PRAGMA foreign_key_list (dqlite doesn't
        enforce FKs by default, but the declared action still round-trips)."""
        return exclusions.open()

    @property
    def foreign_key_constraint_option_reflection_onupdate(self) -> compound:
        """ON UPDATE action reflected; see foreign_key_constraint_option_reflection_ondelete."""
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_noaction(self) -> compound:
        """ON DELETE NO ACTION; see foreign_key_constraint_option_reflection_ondelete."""
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_restrict(self) -> compound:
        """ON DELETE RESTRICT; see foreign_key_constraint_option_reflection_ondelete."""
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_onupdate_restrict(self) -> compound:
        """ON UPDATE RESTRICT; see foreign_key_constraint_option_reflection_ondelete."""
        return exclusions.open()

    @property
    def indexes_check_column_order(self) -> compound:
        """Reflected indexes report columns in declared order (PRAGMA index_info)."""
        return exclusions.open()

    @property
    def temporary_views(self) -> compound:
        """CREATE TEMPORARY VIEW and temp-view reflection."""
        return exclusions.open()

    @property
    def nvarchar_types(self) -> compound:
        """NVARCHAR/NCHAR type reflection via SQLite type affinity."""
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self) -> compound:
        """Numeric trailing significant digits round-trip, e.g. Decimal('1.000')."""
        return exclusions.open()

    @property
    def inline_check_constraint_reflection(self) -> compound:
        """Reflection of column-level (inline) CHECK constraints."""
        return exclusions.open()
