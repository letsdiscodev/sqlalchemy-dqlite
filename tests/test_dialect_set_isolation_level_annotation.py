"""Pin ``set_isolation_level``'s ``level`` annotation to the parent's ``IsolationLevel``, so a
re-widening to ``IsolationLevel | None`` (or ``str | None``) trips."""

from __future__ import annotations

from typing import get_type_hints

from sqlalchemy.engine.interfaces import IsolationLevel

from sqlalchemydqlite.base import DqliteDialect


def test_set_isolation_level_level_param_is_isolation_level() -> None:
    hints = get_type_hints(DqliteDialect.set_isolation_level)
    assert hints["level"] == IsolationLevel, (
        f"set_isolation_level(level=...) annotation must be "
        f"``IsolationLevel`` (matches parent + pysqlite); got "
        f"{hints['level']!r}"
    )
