"""SQLAlchemy test suite requirements for dqlite dialect."""

# mypy: disable-error-code="no-untyped-call"

from typing import Any

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    """Test suite requirements for dqlite dialect.

    Override requirements that dqlite doesn't support.
    """

    @property
    def datetime_literals(self) -> Any:
        """dqlite/SQLite doesn't have native datetime literals."""
        return exclusions.closed()

    @property
    def time_microseconds(self) -> Any:
        """SQLite stores time as text without microseconds."""
        return exclusions.closed()

    @property
    def datetime_historic(self) -> Any:
        """SQLite date range limitation."""
        return exclusions.closed()

    @property
    def unicode_ddl(self) -> Any:
        """SQLite supports unicode in DDL."""
        return exclusions.open()

    @property
    def savepoints(self) -> Any:
        """dqlite supports savepoints."""
        return exclusions.open()

    @property
    def two_phase_transactions(self) -> Any:
        """dqlite doesn't support two-phase transactions."""
        return exclusions.closed()

    @property
    def temp_table_reflection(self) -> Any:
        """SQLite supports temp table reflection."""
        return exclusions.open()
