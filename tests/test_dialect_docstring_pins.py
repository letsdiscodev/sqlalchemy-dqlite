"""Behavioral guard: the dialect carries no ``_timeout`` attribute (the timeout
lives on the dbapi Connection, not the dialect)."""

from __future__ import annotations

from sqlalchemydqlite.base import DqliteDialect


def test_dialect_has_no_timeout_attribute() -> None:
    dialect = DqliteDialect()
    assert not hasattr(dialect, "_timeout")
