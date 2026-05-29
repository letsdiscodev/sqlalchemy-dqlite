"""``do_ping`` returns False on ``ProgrammingError`` (a slot-level fault,
e.g. cross-loop reuse) rather than letting it escape and leave SA's pool
slot half-dead.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.exceptions import ProgrammingError
from sqlalchemydqlite.base import DqliteDialect


def test_do_ping_returns_false_on_programming_error() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = ProgrammingError(
        "AsyncConnection was first used on a different event loop"
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    dialect = DqliteDialect()
    assert dialect.do_ping(conn) is False
    cursor.close.assert_called_once()


def test_do_ping_propagates_real_programmer_bug_when_close_raises_non_caught() -> None:
    # TypeError from cursor() is not connection-level, so it propagates.
    conn = MagicMock()
    conn.cursor.side_effect = TypeError("bad argument")

    dialect = DqliteDialect()
    try:
        dialect.do_ping(conn)
    except TypeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("TypeError should have propagated")
