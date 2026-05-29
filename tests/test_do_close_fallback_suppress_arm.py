"""Pin: ``do_close``'s fallback wraps ``force_close_transport()`` in
``suppress(*_FORCE_CLOSE_TAIL_EXCEPTIONS)`` (never-raises contract); a
non-transport class still propagates — the suppress is narrow, not broad.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi.exceptions import InterfaceError, OperationalError
from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    ("first_exc", "second_exc"),
    [
        (OperationalError("first close failed"), OSError("force_close FIN")),
        (
            InterfaceError("first close interfaceerror"),
            TimeoutError("force_close timeout"),
        ),
        (
            DqliteConnectionError("first transport"),
            ConnectionResetError("force_close reset"),
        ),
        (
            OperationalError("first"),
            OperationalError("second"),
        ),
        (
            OperationalError("first"),
            InterfaceError("second"),
        ),
        (
            OperationalError("first"),
            DqliteConnectionError("second"),
        ),
        # Tuple widens to RuntimeError ("Event loop is closed") and
        # ReferenceError (dead-proxy weakref). Pin both.
        (
            OperationalError("first"),
            RuntimeError("Event loop is closed"),
        ),
        (
            OperationalError("first"),
            ReferenceError("weakly-referenced object no longer exists"),
        ),
    ],
)
def test_do_close_fallback_suppresses_secondary_failure(
    first_exc: BaseException, second_exc: BaseException
) -> None:
    """A secondary transport-class raise from ``force_close_transport`` is
    absorbed; ``do_close`` does not propagate it."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc
    mock_conn.force_close_transport.side_effect = second_exc

    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()


def test_do_close_fallback_propagates_non_transport_class_from_force_close() -> None:
    """A non-transport-class exception from ``force_close_transport`` is NOT
    suppressed."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = OperationalError("first")
    mock_conn.force_close_transport.side_effect = AttributeError("refactor bug")

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)


def test_do_close_fallback_propagates_type_error_from_force_close() -> None:
    """Symmetric negative twin for ``TypeError``."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = OperationalError("first")
    mock_conn.force_close_transport.side_effect = TypeError("type bug")

    with pytest.raises(TypeError, match="type bug"):
        dialect.do_close(mock_conn)


@pytest.mark.parametrize(
    ("first_exc", "second_exc"),
    [
        (OperationalError("first"), OSError("second")),
        (OperationalError("first"), RuntimeError("Event loop is closed")),
        (OperationalError("first"), ReferenceError("dead proxy")),
    ],
)
def test_async_dialect_do_close_fallback_suppresses_secondary_failure(
    first_exc: BaseException, second_exc: BaseException
) -> None:
    """The inherited suppress arm absorbs secondary failures on the async side."""
    dialect = DqliteDialect_aio()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc
    mock_conn.force_close_transport.side_effect = second_exc

    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()
