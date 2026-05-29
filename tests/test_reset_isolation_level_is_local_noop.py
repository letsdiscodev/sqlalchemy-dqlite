"""Pin: ``DqliteDialect.reset_isolation_level`` is a local no-op override.

The inherited ``DefaultDialect.reset_isolation_level`` dispatches to
``set_isolation_level``, which would surface the AUTOCOMMIT rejection on SA's
pool-checkin path the user never initiated. The override keeps the
SERIALIZABLE-only contract symmetric across connect / set / reset."""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_reset_isolation_level_is_defined_locally() -> None:
    """Local override guards against an SA refactor silently regressing the contract."""
    assert "reset_isolation_level" in DqliteDialect.__dict__, (
        "DqliteDialect.reset_isolation_level must be a local override "
        "(not inherited) so SA's pool checkin path does not route "
        "through _assert_and_set_isolation_level + set_isolation_level "
        "and surface the AUTOCOMMIT rejection on a finalize path."
    )


def test_reset_isolation_level_is_noop_and_does_not_dispatch() -> None:
    """The override must not call ``set_isolation_level`` (which would re-fire
    the AUTOCOMMIT rejection)."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    mock_conn = MagicMock()
    dialect.reset_isolation_level(mock_conn)
    assert not mock_conn.method_calls


def test_async_dialect_inherits_local_reset_isolation_level() -> None:
    """The async dialect inherits the override rather than duplicating it."""
    assert DqliteDialect_aio.reset_isolation_level is DqliteDialect.reset_isolation_level
