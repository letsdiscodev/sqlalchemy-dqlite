"""Pin ``set_isolation_level``'s parameter annotation to the parent
contract's literal type.

The SA parent ``Dialect.set_isolation_level`` declares
``level: IsolationLevel`` (a ``Literal[...]`` of the five canonical
level names) in ``sqlalchemy/engine/interfaces.py``. The pysqlite
dialect keeps the same shape; the dqlite dialect now matches that
shape too — the earlier ``IsolationLevel | None`` widening was load-
bearing for a stale ``reset_isolation_level`` path that the dialect's
local no-op override (see ``DqliteDialect.reset_isolation_level``)
disconnects from ``set_isolation_level`` entirely. The runtime body
rejects ``None`` with ``ArgumentError`` for defence-in-depth against
direct callers using ``cast`` or ``# type: ignore``.

Regression guard: if someone re-widens to ``str | None`` (or
re-introduces the ``IsolationLevel | None`` widening) for ergonomics
this test surfaces the contract drift immediately.
"""

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
