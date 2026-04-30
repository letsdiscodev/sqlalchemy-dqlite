"""Pin: ``do_ping`` returns False on any ``InterfaceError`` from
``SELECT 1``, including the cycle-21 code-bearing variants
(``DQLITE_PROTO`` = 1001, ``SQLITE_MISUSE`` = 21,
``SQLITE_RANGE`` = 25).

Asymmetry with ``is_disconnect``: ``is_disconnect`` keeps a
narrow ``InterfaceError`` arm that only catches closed-handle
substrings — caller-side bind misuse (``SQLITE_RANGE`` /
``SQLITE_MISUSE``) on a real query must propagate as a real
error, not be silently rewritten as a disconnect.

Pre-ping is the inverse contract: ANY ``InterfaceError`` on
``SELECT 1`` (a parameterless statement that should never raise
caller-side bind errors) means the slot is dead. The
asymmetry is deliberate.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "code",
    [1001, 21, 25],  # DQLITE_PROTO, SQLITE_MISUSE, SQLITE_RANGE
    ids=["DQLITE_PROTO", "SQLITE_MISUSE", "SQLITE_RANGE"],
)
def test_do_ping_returns_false_on_code_bearing_interface_error(code: int) -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.execute.side_effect = InterfaceError(
        "server-emitted code-bearing variant", code=code, raw_message="x"
    )
    conn.cursor.return_value = cur

    d = DqliteDialect()
    assert d.do_ping(conn) is False
    cur.close.assert_called_once_with()
