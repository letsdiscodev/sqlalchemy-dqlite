"""Pin: ``do_close`` calls the dbapi ``close()`` directly — no ``timeout=``
kwarg (the dbapi's signature has none; the bogus kwarg made every dispose
force-close via the broad ``except``). Suppression is narrowed to the
transport-class tuple so programmer bugs surface.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.base import DqliteDialect


def test_do_close_calls_dbapi_close_with_no_kwargs() -> None:
    """Happy path: ``close()`` is called with no arguments."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()


def test_do_close_calls_close_when_close_timeout_attr_missing() -> None:
    """close() is called the same way whether or not ``_close_timeout`` exists."""
    dialect = DqliteDialect()
    mock_conn = MagicMock(spec=["close"])
    mock_conn.close = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()


def test_do_close_does_not_fall_through_to_force_close_on_happy_path() -> None:
    """On the happy path ``force_close_transport`` is NOT called."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock()
    mock_conn.force_close_transport = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()
    mock_conn.force_close_transport.assert_not_called()


@pytest.mark.parametrize(
    "transport_exc",
    [
        TimeoutError("close timed out"),
        ConnectionResetError("close FIN"),
        OSError("close OSError"),
    ],
    ids=["TimeoutError", "ConnectionResetError", "OSError"],
)
def test_do_close_transport_failure_falls_through_to_force_close(
    transport_exc: BaseException,
) -> None:
    """Transport-class failure during close falls through to
    ``force_close_transport`` so the slot releases."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock(side_effect=transport_exc)
    mock_conn.force_close_transport = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()
    mock_conn.force_close_transport.assert_called_once_with()


def test_do_close_programmer_bug_raises_instead_of_force_close_fallback() -> None:
    """A programmer bug (AttributeError/TypeError) propagates rather than
    falling through to the ``force_close_transport`` fallback."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock(side_effect=AttributeError("refactor bug"))
    mock_conn.force_close_transport = MagicMock()

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_not_called()
