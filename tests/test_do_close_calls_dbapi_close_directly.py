"""Pin: ``DqliteDialect.do_close`` calls the dbapi
``Connection.close()`` *directly* — without the bogus ``timeout=``
kwarg the dbapi does not accept.

Pre-fix, ``do_close`` called ``dbapi_connection.close(timeout=close_timeout)``,
but the dbapi's ``Connection.close()`` signature is
``def close(self) -> None:`` (no ``timeout`` parameter). Every call
raised ``TypeError("close() got an unexpected keyword argument
'timeout'")``, which the broad ``except Exception`` swallowed and
fell back to ``force_close_transport()`` — meaning the dialect's
"graceful close bounded by close_timeout" path was dead code on every
checkin / ``engine.dispose()``. Operationally every dispose force-
closed despite the documented contract.

The dbapi already captures ``close_timeout`` at construction (it
bounds the internal thread join in ``Connection.close``), so the
correct behaviour is a plain ``dbapi_connection.close()`` call.

The pin asserts:

1. ``do_close`` happy path calls ``close()`` with no kwargs.
2. ``do_close`` does NOT fall through to ``force_close_transport``
   on the happy path.
3. The exception suppression is narrowed from the broad ``except
   Exception:`` to the transport-class tuple, so a programmer-bug
   (``AttributeError`` / ``TypeError`` from a refactor) is no longer
   silently swallowed into the ``force_close_transport`` fallback.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.base import DqliteDialect


def test_do_close_calls_dbapi_close_with_no_kwargs() -> None:
    """Happy path: the dialect calls ``dbapi_connection.close()``
    with no positional or keyword arguments — matching the dbapi's
    actual signature ``def close(self) -> None:``.
    """
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0  # would have triggered the bogus kwarg pre-fix
    mock_conn.close = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()


def test_do_close_calls_close_when_close_timeout_attr_missing() -> None:
    """The ``getattr(dbapi_connection, "_close_timeout", None)`` lookup
    is preserved as defensive shape but never gates the call shape
    anymore — close() is called the same way either way.
    """
    dialect = DqliteDialect()
    mock_conn = MagicMock(spec=["close"])
    mock_conn.close = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()


def test_do_close_does_not_fall_through_to_force_close_on_happy_path() -> None:
    """Pre-fix, every happy-path call hit the bogus-kwarg TypeError,
    got caught by the broad ``except Exception:``, and fell through
    to ``force_close_transport()`` — meaning the graceful-close
    contract was dead code. Pin: on the happy path,
    ``force_close_transport`` is NOT called.
    """
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
    """Transport-class failure during graceful close still falls
    through to ``force_close_transport`` so the slot releases. The
    fallback discipline is the whole reason ``do_close`` exists —
    only the call shape changed; the fallback contract is preserved.
    """
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock(side_effect=transport_exc)
    mock_conn.force_close_transport = MagicMock()

    dialect.do_close(mock_conn)

    mock_conn.close.assert_called_once_with()
    mock_conn.force_close_transport.assert_called_once_with()


def test_do_close_programmer_bug_raises_instead_of_force_close_fallback() -> None:
    """An ``AttributeError`` / ``TypeError`` from a refactor is a
    programmer bug, not a transport failure. The narrowed except
    tuple lets these propagate so a real bug doesn't get silently
    swallowed into the ``force_close_transport`` fallback.

    Pre-fix's broad ``except Exception:`` swallowed these too —
    operators saw graceful disposes that were actually masking a
    real bug.
    """
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn._close_timeout = 2.0
    mock_conn.close = MagicMock(side_effect=AttributeError("refactor bug"))
    mock_conn.force_close_transport = MagicMock()

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)

    # The fallback should NOT have run — the bug surfaces, not gets
    # masked.
    mock_conn.force_close_transport.assert_not_called()
