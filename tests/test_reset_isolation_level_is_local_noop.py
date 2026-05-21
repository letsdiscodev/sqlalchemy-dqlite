"""Pin: ``DqliteDialect.reset_isolation_level`` is a local no-op
override.

The dialect's ``_isolation_lookup`` truthful set is
``{"SERIALIZABLE": 0}`` (single-level) but
``get_isolation_level_values`` advertises
``["SERIALIZABLE", "AUTOCOMMIT"]`` as a diagnostic-routing channel
so ``set_isolation_level`` can route AUTOCOMMIT to a dedicated
rejection message. SA's inherited ``DefaultDialect.reset_isolation_level``
calls ``_assert_and_set_isolation_level`` which validates against
the values list and dispatches to ``set_isolation_level``. If a
caller used ``Connection.execution_options(isolation_level="AUTOCOMMIT")``,
the inherited reset path would surface the AUTOCOMMIT-rejection from
SA's pool checkin — a SA-internal path the user did not initiate.

The override short-circuits checkin to a no-op so the
SERIALIZABLE-only contract is symmetric across connect / set / reset.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_reset_isolation_level_is_defined_locally() -> None:
    """Pin local override — defense against an SA refactor of
    ``DefaultDialect.reset_isolation_level`` silently regressing the
    contract."""
    assert "reset_isolation_level" in DqliteDialect.__dict__, (
        "DqliteDialect.reset_isolation_level must be a local override "
        "(not inherited) so SA's pool checkin path does not route "
        "through _assert_and_set_isolation_level + set_isolation_level "
        "and surface the AUTOCOMMIT rejection on a finalize path."
    )


def test_reset_isolation_level_is_noop_and_does_not_dispatch() -> None:
    """Call the override directly: it MUST NOT call
    ``set_isolation_level`` (which would re-fire AUTOCOMMIT rejection
    for a slot that had it on)."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    mock_conn = MagicMock()
    # ``reset_isolation_level`` returns None implicitly; the assertion
    # below pins that no underlying connection method was called.
    dialect.reset_isolation_level(mock_conn)
    # No mutating call should have been issued on the connection.
    assert not mock_conn.method_calls


def test_async_dialect_inherits_local_reset_isolation_level() -> None:
    """The async dialect inherits the override (we did not duplicate
    on the subclass). Pin the inheritance is intact so a future
    refactor that adds an async-specific override has to consider
    the contract."""
    assert DqliteDialect_aio.reset_isolation_level is DqliteDialect.reset_isolation_level
