"""``do_ping`` returns False on ``ProgrammingError`` from the dbapi.

Two deterministic dbapi paths raise ``ProgrammingError`` for a slot
that is permanently unusable by the pool:

- ``AsyncConnection._ensure_locks`` when the connection is reused on
  a different event loop.
- ``_build_and_connect`` remapped ``ClusterPolicyError`` (addressed
  separately, but the pool surface is the same shape).

Neither is a real programmer bug on the ``SELECT 1`` query — it is a
slot-level fault. Without this catch, the exception escapes
``do_ping`` and SA's pool leaves the slot in a half-dead state.
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
    # ``TypeError`` from cursor() (not a connection-level exception) must
    # still propagate out of do_ping — only connection-level exception
    # classes are considered "dead socket".
    conn = MagicMock()
    conn.cursor.side_effect = TypeError("bad argument")

    dialect = DqliteDialect()
    try:
        dialect.do_ping(conn)
    except TypeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("TypeError should have propagated")
