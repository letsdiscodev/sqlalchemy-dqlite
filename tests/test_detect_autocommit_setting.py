"""``DqliteDialect.detect_autocommit_setting`` returns False unconditionally.

dqlite has no AUTOCOMMIT mode (every statement goes through Raft
consensus; see ``set_isolation_level`` rejection). The dqlite dbapi
``Connection`` exposes ``isolation_level`` returning ``None`` for
stdlib pre-3.12 parity. Without this override, the pysqlite parent's
probe (``dbapi_conn.isolation_level is None``) would succeed and flip
on SA's ``skip_autocommit_rollback`` short-circuit
(``sqlalchemy/engine/default.py::do_rollback`` /
``base.py:1115-1124``) — which was designed for stdlib's
connection-level auto-BEGIN, not for dqlite's explicit-BEGIN wire
discipline. Override to return False so the SA-managed BEGIN/COMMIT
lifecycle is preserved.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


class TestDetectAutocommitSetting:
    def test_dqlite_dialect_returns_false_on_arbitrary_object(self) -> None:
        # MagicMock has no ``isolation_level`` attribute set explicitly,
        # but auto-attribute makes the inherited probe falsely succeed.
        # Use ``object()`` to expose the real failure mode of the
        # inherited method.
        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(object()) is False  # type: ignore[arg-type]

    def test_dqlite_dialect_returns_false_on_magic_mock(self) -> None:
        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(MagicMock()) is False

    def test_dqlite_dialect_does_not_touch_dbapi_attribute(self) -> None:
        # Override must not access ``isolation_level`` on the dbapi
        # connection — that probe is the bug we are fixing. Use a
        # class-level descriptor (not ``MagicMock.side_effect``, which
        # only fires on call, not on attribute access) so any read
        # raises and surfaces an accidental probe.
        class _Probe:
            @property
            def isolation_level(self) -> object:
                raise AssertionError("must not probe isolation_level")

        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(_Probe()) is False  # type: ignore[arg-type]

    def test_async_dialect_inherits_override(self) -> None:
        dialect = DqliteDialect_aio()
        assert dialect.detect_autocommit_setting(object()) is False  # type: ignore[arg-type]

    def test_dbapi_connection_exposes_isolation_level_returning_none(self) -> None:
        """Defence pin: the dbapi ``Connection.isolation_level`` reads
        as ``None`` (stdlib parity stub), so SA's pysqlite-style probe
        ``dbapi_conn.isolation_level is None`` succeeds and would flip
        on ``skip_autocommit_rollback`` without this dialect's
        ``detect_autocommit_setting`` override.

        Pin the actual VALUE returned by the descriptor — not just the
        descriptor's existence — so a future regression that flips
        the body to e.g. ``return "DEFERRED"`` fails this test.
        """
        from dqlitedbapi.connection import Connection as DqliteSyncConnection

        # Read the value through an instance so the property descriptor
        # actually fires (as opposed to checking the descriptor itself).
        instance_unused = DqliteSyncConnection.__new__(DqliteSyncConnection)
        assert instance_unused.isolation_level is None

    def test_async_dbapi_connection_isolation_level_also_returns_none(self) -> None:
        """Sibling pin on the async surface — the same SA probe runs
        through the async adapter."""
        from dqlitedbapi.aio.connection import (
            AsyncConnection as DqliteAsyncConnection,
        )

        instance_unused = DqliteAsyncConnection.__new__(DqliteAsyncConnection)
        assert instance_unused.isolation_level is None

    def test_override_remains_load_bearing_against_isolation_level_eq_none(self) -> None:
        """Pin: the override returns False even if the dbapi probe
        says ``isolation_level is None`` (which it does post-stdlib-
        parity stub). Without the override, SA would flip on
        ``skip_autocommit_rollback`` and bypass our explicit-BEGIN
        wire discipline."""

        class _StdlibParityProbe:
            isolation_level = None

        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(_StdlibParityProbe()) is False  # type: ignore[arg-type]
