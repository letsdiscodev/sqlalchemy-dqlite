"""Pin: SA ``do_ping`` re-raises bare ``DatabaseError`` whose code
is OUTSIDE ``_BARE_DBE_DISCONNECT_CODES`` (or ``None``).

Existing ``test_do_ping_database_error.py`` covers the inside-the-set
codes (11/24/26 → ``return False``). This file pins the
complementary branch: a bare DatabaseError with any other code or
``code=None`` propagates so a genuine fault surfaces instead of
being silently rewritten as "ping failed; reconnect the slot."
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import DatabaseError
from sqlalchemydqlite.base import _BARE_DBE_DISCONNECT_CODES, DqliteDialect


class TestDoPingReraisesNonDisconnectDatabaseError:
    def test_re_raises_with_non_disconnect_code(self) -> None:
        dialect = DqliteDialect()
        cursor = MagicMock()
        # Pick a code NOT in the disconnect set.
        non_disconnect = 99
        assert non_disconnect not in _BARE_DBE_DISCONNECT_CODES
        cursor.execute.side_effect = DatabaseError("schema-trigger explosion", non_disconnect)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(DatabaseError, match="schema-trigger"):
            dialect.do_ping(conn)

    def test_re_raises_with_code_none(self) -> None:
        """``code=None`` is also outside the disconnect set; must
        propagate so the underlying fault surfaces."""
        dialect = DqliteDialect()
        cursor = MagicMock()
        cursor.execute.side_effect = DatabaseError("unknown DB fault", None)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with pytest.raises(DatabaseError, match="unknown"):
            dialect.do_ping(conn)
