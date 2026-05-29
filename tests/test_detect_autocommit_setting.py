"""detect_autocommit_setting returns False unconditionally. dqlite has no
AUTOCOMMIT mode; without the override the pysqlite probe (isolation_level is None)
would flip on skip_autocommit_rollback and bypass our explicit-BEGIN discipline."""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


class TestDetectAutocommitSetting:
    def test_dqlite_dialect_returns_false_on_arbitrary_object(self) -> None:
        # object() (not MagicMock, whose auto-attribute would mask it) exposes
        # the inherited probe's real failure mode.
        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(object()) is False  # type: ignore[arg-type]

    def test_dqlite_dialect_returns_false_on_magic_mock(self) -> None:
        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(MagicMock()) is False

    def test_dqlite_dialect_does_not_touch_dbapi_attribute(self) -> None:
        # A class-level descriptor (not MagicMock.side_effect, which fires only on
        # call) makes any read of isolation_level raise, surfacing an accidental probe.
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
        """The dbapi Connection.isolation_level reads as None (stdlib parity stub),
        the value SA's probe keys on. Pin the value, not just the descriptor."""
        from dqlitedbapi.connection import Connection as DqliteSyncConnection

        instance_unused = DqliteSyncConnection.__new__(DqliteSyncConnection)
        # The getter raises InterfaceError on a closed connection; __new__ skips
        # __init__, so set _closed explicitly to reach the property body.
        instance_unused._closed = False
        assert instance_unused.isolation_level is None

    def test_async_dbapi_connection_isolation_level_also_returns_none(self) -> None:
        """Sibling pin on the async surface."""
        from dqlitedbapi.aio.connection import (
            AsyncConnection as DqliteAsyncConnection,
        )

        instance_unused = DqliteAsyncConnection.__new__(DqliteAsyncConnection)
        # See sync sibling: _closed must be set before the property fires.
        instance_unused._closed = False
        assert instance_unused.isolation_level is None

    def test_override_remains_load_bearing_against_isolation_level_eq_none(self) -> None:
        """The override returns False even when the probe sees isolation_level is None."""

        class _StdlibParityProbe:
            isolation_level = None

        dialect = DqliteDialect()
        assert dialect.detect_autocommit_setting(_StdlibParityProbe()) is False  # type: ignore[arg-type]
