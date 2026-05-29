"""is_disconnect recognises the dbapi's "used after fork" InterfaceError
so the SA pool invalidates the fork-inherited slot.

The substring is "used after fork", not the bare token "fork", to avoid
false positives on user messages.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect


def _make_dialect() -> DqliteDialect:
    return DqliteDialect()


def test_used_after_fork_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "Connection used after fork; reconstruct from configuration "
        "in the target process. (created in pid 1234, current pid 5678)"
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_async_used_after_fork_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "AsyncConnection used after fork; reconstruct from "
        "configuration in the target process. (created in pid 1234, "
        "current pid 5678)"
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_unrelated_fork_word_not_classified_as_disconnect() -> None:
    """An InterfaceError with an unrelated "fork" must not trip."""
    dialect = _make_dialect()
    e = InterfaceError("forked workflow not supported by this trigger")
    assert dialect.is_disconnect(e, None, None) is False
