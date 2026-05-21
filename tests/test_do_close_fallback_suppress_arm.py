"""Pin: ``DqliteDialect.do_close``'s fallback wraps
``force_close_transport()`` in ``contextlib.suppress(*_FORCE_CLOSE_TAIL_EXCEPTIONS)``.
The suppress arm is the load-bearing "never raises" contract — by the
time the fallback fires, the primary close already failed and SA's pool
slot must release regardless.

Existing tests stub ``force_close_transport = MagicMock()`` with no
side effect; the suppress arm is never traversed. These pins drive
``force_close_transport.side_effect`` with each exception class the
production tuple covers and assert ``do_close`` returns ``None``
without raising. A negative twin pins that non-transport classes (e.g.
``AttributeError`` from a refactor) DO propagate — the suppress is
narrow, not broad.
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
        # ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` widens to include RuntimeError
        # (cross-loop ``RuntimeError("Event loop is closed")`` from a
        # defunct loop) and ReferenceError (dead-proxy weakref on a
        # half-collected AsyncAdaptedConnection). Pin both.
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
    """The dialect-level suppress absorbs a secondary transport-class
    raise from ``force_close_transport`` itself. SA's pool slot must
    still release; ``do_close`` must not propagate the secondary
    failure."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc
    mock_conn.force_close_transport.side_effect = second_exc

    # Must NOT raise.
    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()


def test_do_close_fallback_propagates_non_transport_class_from_force_close() -> None:
    """Negative twin: a non-transport-class exception from
    ``force_close_transport`` (programmer bug — ``AttributeError`` /
    ``TypeError``) is NOT suppressed. The narrow suppress tuple is
    designed to let real bugs surface."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = OperationalError("first")
    mock_conn.force_close_transport.side_effect = AttributeError("refactor bug")

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)


def test_do_close_fallback_propagates_type_error_from_force_close() -> None:
    """Symmetric negative twin for ``TypeError`` (also a programmer-
    bug class outside ``_FORCE_CLOSE_TAIL_EXCEPTIONS``)."""
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
    """The async dialect inherits ``do_close`` from the sync sibling;
    pin that the inherited suppress arm absorbs secondary failures
    from ``force_close_transport`` on the async side too."""
    dialect = DqliteDialect_aio()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc
    mock_conn.force_close_transport.side_effect = second_exc

    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()
