"""AsyncAdaptedCursor.fetchmany must mirror stdlib sqlite3 and the
underlying dqlitedbapi cursor's negative-size contract: ``size < 0``
returns all remaining rows.

The dbapi-layer cursors (``dqlitedbapi.Cursor.fetchmany`` and
``dqlitedbapi.aio.AsyncCursor.fetchmany``) were aligned with the
stdlib via the resolved cross-driver-parity issue. The SA async
adapter cursor was left at the older strict-reject contract from an
earlier triage. Cross-driver code that uses ``fetchmany(-1)`` as
"drain all" (stdlib + libpq + psycopg-style convention) breaks at the
SA layer despite working through the dbapi layer directly. Align the
SA leaf so the contract is uniform across all three layers.
"""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_fetchmany_negative_size_returns_all_remaining() -> None:
    """``size < 0`` returns all remaining rows, mirroring stdlib
    ``sqlite3.Cursor.fetchmany(-1)`` and the underlying
    ``dqlitedbapi`` cursors. The deque is fully drained.
    """
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque([("a",), ("b",), ("c",)])

    rows = list(cur.fetchmany(-1))
    assert rows == [("a",), ("b",), ("c",)]
    assert len(cur._rows) == 0


def test_fetchmany_negative_size_on_empty_deque_returns_empty() -> None:
    """``fetchmany(-1)`` on an empty deque returns ``[]`` (stdlib
    parity — drain-all on already-empty is a no-op).
    """
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque()

    assert list(cur.fetchmany(-1)) == []


def test_fetchmany_zero_size_returns_empty() -> None:
    """Existing behaviour: size=0 returns [] without consuming the deque.
    Distinct from ``size < 0`` which drains; aligns with the dbapi
    cursor's documented size=0 divergence note (deterministic empty
    return, no draining)."""
    conn = MagicMock()
    cur = AsyncAdaptedCursor(conn)
    cur._rows = deque([("a",)])
    assert list(cur.fetchmany(0)) == []
    assert len(cur._rows) == 1
