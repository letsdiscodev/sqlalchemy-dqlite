"""Pin: ``do_ping`` returns False on bare ``DatabaseError`` from the dbapi.

``_classify_operational`` routes SQLite codes 11/24/26
(CORRUPT/FORMAT/NOTADB) to bare ``DatabaseError`` (not
``OperationalError``). The ``do_ping`` except list must catch
``DatabaseError`` so a slot whose backing node is corrupt is reported
as "not usable now" — pre-ping's contract — instead of leaking the SQL
exception past the safety net.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.exceptions import DatabaseError
from sqlalchemydqlite.base import DqliteDialect


def test_do_ping_returns_false_on_database_error_corrupt() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = DatabaseError("database disk image is malformed", code=11)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    assert DqliteDialect().do_ping(conn) is False
    cursor.close.assert_called_once()


def test_do_ping_returns_false_on_database_error_format() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = DatabaseError("database is malformed", code=24)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_database_error_notadb() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = DatabaseError("file is not a database", code=26)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_swallows_database_error_from_cursor_close() -> None:
    """Inner finally must also catch ``DatabaseError`` so a CORRUPT
    response on ``cursor.close`` doesn't crash the probe."""
    cursor = MagicMock()
    cursor.execute.return_value = None
    cursor.close.side_effect = DatabaseError("database disk image is malformed", code=11)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    # Returns True (execute succeeded); close failure is debug-logged
    # not propagated.
    assert DqliteDialect().do_ping(conn) is True
