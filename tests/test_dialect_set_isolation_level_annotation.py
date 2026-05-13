"""Pin ``set_isolation_level``'s parameter annotation to the parent
contract's literal type plus ``None``.

The SA parent ``Dialect.set_isolation_level`` declares
``level: IsolationLevel`` (a ``Literal[...]`` of the five canonical
level names) in
``sqlalchemy/engine/interfaces.py``. The pysqlite dialect keeps the
same shape. The dqlite dialect deliberately widens to also accept
``None`` (load-bearing for ``reset_isolation_level`` on harnesses
that bypass ``initialize()`` — see the method's docstring), but the
annotation should not widen further to ``str``: type checkers should
catch a non-Literal string at the call site, with the runtime
``ArgumentError`` fall-through staying as defence-in-depth for
callers using ``cast`` or ``# type: ignore``.

Regression guard: if someone re-widens to ``str | None`` for
ergonomics, this test surfaces the contract drift immediately.
"""

from __future__ import annotations

from typing import get_type_hints

from sqlalchemy.engine.interfaces import IsolationLevel

from sqlalchemydqlite.base import DqliteDialect


def test_set_isolation_level_level_param_is_isolation_level_or_none() -> None:
    hints = get_type_hints(DqliteDialect.set_isolation_level)
    assert hints["level"] == IsolationLevel | None, (
        f"set_isolation_level(level=...) annotation must be "
        f"``IsolationLevel | None`` (matches parent + pysqlite, "
        f"plus the documented ``None`` widening); got {hints['level']!r}"
    )
