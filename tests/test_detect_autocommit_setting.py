"""``DqliteDialect.detect_autocommit_setting`` returns False unconditionally.

dqlite has no AUTOCOMMIT mode (every statement goes through Raft
consensus; see ``set_isolation_level`` rejection). The pysqlite parent's
``detect_autocommit_setting`` probes ``dbapi_conn.isolation_level``,
but the dqlite dbapi ``Connection`` deliberately does NOT expose that
attribute, so the inherited probe raises ``AttributeError`` from inside
SA's ``skip_autocommit_rollback`` path. Override to return False so the
probe is a safe no-op rather than an exception.

Triggered by ``create_engine(..., skip_autocommit_rollback=True)`` (see
``sqlalchemy/engine/default.py::do_rollback`` and ``base.py:1115-1124``).
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
