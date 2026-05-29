"""``DqliteDialect.set_isolation_level`` is case-insensitive on the level arg:
SA upper-cases before dispatch, but direct callers may pass mixed case."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "level",
    ["SERIALIZABLE", "serializable", "Serializable", "sErIaLiZaBlE"],
)
def test_set_isolation_level_serializable_case_insensitive(level: str) -> None:
    """Any case form of SERIALIZABLE is accepted as a no-op."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    dialect.set_isolation_level(conn, level)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "level",
    ["AUTOCOMMIT", "autocommit", "Autocommit"],
)
def test_set_isolation_level_autocommit_rejected_case_insensitive(level: str) -> None:
    """Any case form of AUTOCOMMIT routes through the dedicated rejection."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        dialect.set_isolation_level(conn, level)  # type: ignore[arg-type]


def test_set_isolation_level_unknown_string_rejected() -> None:
    """Unknown levels hit the generic rejection, reported verbatim."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match=r"requested level 'READ COMMITTED'"):
        dialect.set_isolation_level(conn, "READ COMMITTED")


def test_set_isolation_level_none_rejected() -> None:
    """``None`` is rejected (pysqlite parity); the SA-internal reset path is
    handled by the ``reset_isolation_level`` no-op override instead."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match="non-None"):
        dialect.set_isolation_level(conn, None)  # type: ignore[arg-type]


def test_set_isolation_level_non_string_rejected() -> None:
    """A non-string value raises ArgumentError rather than crashing in .upper()."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match="not a string"):
        dialect.set_isolation_level(conn, 42)  # type: ignore[arg-type]
