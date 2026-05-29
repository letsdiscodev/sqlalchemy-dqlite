"""``set_isolation_level(conn, None)`` raises ``ArgumentError`` (pysqlite
parity) instead of a silent no-op; the SA reset path that the old
silent-accept arm protected now goes through ``reset_isolation_level``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_set_isolation_level_none_raises_argument_error() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="non-None"):
        dialect.set_isolation_level(MagicMock(), None)  # type: ignore[arg-type]


def test_reset_isolation_level_still_callable_on_uninitialised_dialect() -> None:
    """The no-op override must not raise even before ``initialize()``."""
    dialect = DqliteDialect()
    dialect.reset_isolation_level(MagicMock())


def test_set_isolation_level_serializable_still_accepted() -> None:
    """The reject-None change must not affect the SERIALIZABLE accept path."""
    dialect = DqliteDialect()
    dialect.set_isolation_level(MagicMock(), "SERIALIZABLE")
