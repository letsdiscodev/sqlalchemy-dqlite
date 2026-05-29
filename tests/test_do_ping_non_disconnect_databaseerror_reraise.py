"""Pin: ``do_ping`` re-raises bare ``DatabaseError`` whose code is outside
``_BARE_DBE_DISCONNECT_CODES`` (or ``None``) so a genuine fault surfaces."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import DatabaseError
from sqlalchemydqlite.base import _BARE_DBE_DISCONNECT_CODES, DqliteDialect


class TestDoPingReraisesNonDisconnectDatabaseError:
    def test_re_raises_with_non_disconnect_code(self) -> None:
        dialect = DqliteDialect()
        cursor = MagicMock()
        non_disconnect = 99
        assert non_disconnect not in _BARE_DBE_DISCONNECT_CODES
        cursor.execute.side_effect = DatabaseError("schema-trigger explosion", non_disconnect)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(DatabaseError, match="schema-trigger"):
            dialect.do_ping(conn)

    def test_re_raises_with_code_none(self) -> None:
        dialect = DqliteDialect()
        cursor = MagicMock()
        cursor.execute.side_effect = DatabaseError("unknown DB fault", None)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(DatabaseError, match="unknown"):
            dialect.do_ping(conn)
