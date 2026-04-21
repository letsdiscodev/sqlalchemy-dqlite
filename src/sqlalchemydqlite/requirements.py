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
        """dqlite/SQLite doesn't have native datetime literals."""
        return exclusions.closed()

    @property
    def time_microseconds(self) -> compound:
        """SQLite stores time as text without microseconds."""
        return exclusions.closed()

    @property
    def datetime_historic(self) -> compound:
        """SQLite date range limitation."""
        return exclusions.closed()

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
    def temp_table_reflection(self) -> compound:
        """SQLite supports temp table reflection."""
        return exclusions.open()

    # --- Baseline declarations mirroring SQLite behavior ------------
    # These don't change the effective default (SuiteRequirements already
    # returns exclusions.open() for most), but make the dqlite contract
    # explicit so a future maintainer running the SQLAlchemy compliance
    # suite has a single source of truth to adjust.

    @property
    def cte(self) -> compound:
        """Common Table Expressions (WITH)."""
        return exclusions.open()

    @property
    def window_functions(self) -> compound:
        """SQL window functions (OVER / PARTITION BY)."""
        return exclusions.open()

    @property
    def returning(self) -> compound:
        """RETURNING clause on DML."""
        return exclusions.open()

    @property
    def insert_from_select(self) -> compound:
        """INSERT INTO ... SELECT."""
        return exclusions.open()

    @property
    def on_update_or_delete_cascades(self) -> compound:
        """ON UPDATE/DELETE CASCADE foreign-key actions."""
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
    def supports_empty_inserts(self) -> compound:
        """INSERT INTO t DEFAULT VALUES. SQLite supports it; dqlite inherits."""
        return exclusions.open()

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
