"""Pin: ``do_ping`` classifies a failure of ``dbapi_connection.cursor()``
itself as ping-fail (returns ``False``).

The sync ``do_ping`` previously called ``cursor = dbapi_connection.cursor()``
OUTSIDE its inner try/except, so any failure of ``cursor()`` propagated
past the dialect's classification. SA's ``_do_ping_w_event`` only
catches ``loaded_dbapi.Error``, and ``OSError`` is not a dbapi-Error
subclass — under a live socket-RST mid-checkout the OSError leaked to
the SA caller instead of returning a clean ping-fail.

Hoisting the call inside the outer try routes ``OSError``,
``InterfaceError`` (closed connection), ``DqliteConnectionError``, and
``ProgrammingError`` through the same disconnect-classification arm
as ``cursor.execute(...)`` failures.

Programming bugs (TypeError, AttributeError, ...) on ``cursor()`` must
still propagate so refactors can't silently break the probe.
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
    """Live socket-RST mid-``cursor()``: OSError must classify as
    ping-fail rather than propagating to the SA caller."""
    conn = MagicMock()
    conn.cursor.side_effect = OSError("ECONNRESET")

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_interface_error_from_cursor() -> None:
    """``cursor()`` on a closed connection raises InterfaceError —
    classify as ping-fail."""
    conn = MagicMock()
    conn.cursor.side_effect = InterfaceError("Connection is closed")

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_programming_error_from_cursor() -> None:
    """Cross-loop reuse from ``AsyncConnection._ensure_locks`` raises
    ProgrammingError on ``cursor()`` — classify as ping-fail."""
    conn = MagicMock()
    conn.cursor.side_effect = ProgrammingError(
        "AsyncConnection was first used on a different event loop"
    )

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_returns_false_on_dqlite_connection_error_from_cursor() -> None:
    """A transport-class fault during ``cursor()`` classifies as
    ping-fail; the slot is unusable."""
    conn = MagicMock()
    conn.cursor.side_effect = DqliteConnectionError("connection closed")

    assert DqliteDialect().do_ping(conn) is False


def test_do_ping_propagates_typeerror_from_cursor() -> None:
    """Negative pin: a real programmer bug (TypeError on ``cursor()``)
    must still propagate so refactors that break the probe surface
    instead of silently masking as ping-fail."""
    conn = MagicMock()
    conn.cursor.side_effect = TypeError("bad argument")

    with pytest.raises(TypeError, match="bad argument"):
        DqliteDialect().do_ping(conn)


def test_do_ping_does_not_call_close_when_cursor_failed() -> None:
    """When ``cursor()`` itself raised, there is no cursor to close —
    the finally arm must skip ``cursor.close()`` rather than
    NameError on an unbound name or call ``.close()`` on a sentinel."""
    conn = MagicMock()
    conn.cursor.side_effect = OSError("ECONNRESET")

    assert DqliteDialect().do_ping(conn) is False
    # The mock's cursor() raised; nothing was returned to call close on.
    # Only assertion is that do_ping completed cleanly (above).
