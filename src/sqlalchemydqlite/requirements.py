"""SQLAlchemy test suite requirements for dqlite dialect."""

from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    """Test suite requirements for dqlite dialect.

    Override requirements that dqlite doesn't support.
    """

    @property
    def datetime_literals(self) -> bool:
        """dqlite/SQLite doesn't have native datetime literals."""
        return False

    @property
    def time_microseconds(self) -> bool:
        """SQLite stores time as text without microseconds."""
        return False

    @property
    def datetime_historic(self) -> bool:
        """SQLite date range limitation."""
        return False

    @property
    def unicode_ddl(self) -> bool:
        """SQLite supports unicode in DDL."""
        return True

    @property
    def savepoints(self) -> bool:
        """dqlite supports savepoints."""
        return True

    @property
    def two_phase_transactions(self) -> bool:
        """dqlite doesn't support two-phase transactions."""
        return False

    @property
    def temp_table_reflection(self) -> bool:
        """SQLite supports temp table reflection."""
        return True
