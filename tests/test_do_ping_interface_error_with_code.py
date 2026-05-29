"""Pin: ``do_ping`` returns False on any ``InterfaceError`` from ``SELECT 1``.

Unlike ``is_disconnect`` (narrow arm), pre-ping treats ANY InterfaceError on the
parameterless SELECT 1 as a dead slot, since it cannot be caller-side bind misuse.
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
