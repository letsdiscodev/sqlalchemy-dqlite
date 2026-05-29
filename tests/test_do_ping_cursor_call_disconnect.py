"""Pin: ``do_ping`` classifies a failure of ``cursor()`` itself as ping-fail.
The call is inside the outer try so OSError (not a dbapi-Error, would leak
past SA's ``_do_ping_w_event``) and other transport faults classify
cleanly; programming bugs still propagate.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import (
    InterfaceError,
    ProgrammingError,
)
from sqlalchemydqlite.base import DqliteDialect


def test_do_ping_returns_false_on_oserror_from_cursor() -> None:
    """OSError from ``cursor()`` classifies as ping-fail, not propagated."""
    conn = MagicMock()
    conn.cursor.side_effect = OSError("ECONNRESET")

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_interface_error_from_cursor() -> None:
    """InterfaceError from ``cursor()`` classifies as ping-fail."""
    conn = MagicMock()
    conn.cursor.side_effect = InterfaceError("Connection is closed")

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_programming_error_from_cursor() -> None:
    """ProgrammingError (cross-loop reuse) from ``cursor()`` is ping-fail."""
    conn = MagicMock()
    conn.cursor.side_effect = ProgrammingError(
        "AsyncConnection was first used on a different event loop"
    )

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_dqlite_connection_error_from_cursor() -> None:
    """A transport-class fault during ``cursor()`` is ping-fail."""
    conn = MagicMock()
    conn.cursor.side_effect = DqliteConnectionError("connection closed")

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_propagates_typeerror_from_cursor() -> None:
    """A programmer bug (TypeError on ``cursor()``) still propagates."""
    conn = MagicMock()
    conn.cursor.side_effect = TypeError("bad argument")

    with pytest.raises(TypeError, match="bad argument"):
        DqliteDialect().do_ping(conn)


def test_do_ping_does_not_call_close_when_cursor_failed() -> None:
    """When ``cursor()`` raised, the finally arm skips ``cursor.close()``
    (no NameError, no close on a sentinel)."""
    conn = MagicMock()
    conn.cursor.side_effect = OSError("ECONNRESET")

    assert DqliteDialect().do_ping(conn) is False
