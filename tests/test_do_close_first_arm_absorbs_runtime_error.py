"""Pin: ``do_close``'s first-close ``except`` arm uses the wider
``_FORCE_CLOSE_TAIL_EXCEPTIONS`` so RuntimeError("Event loop is closed")
and ReferenceError (reachable from cross-loop dispose) don't abort SA's
pool finalize; ``force_close_transport`` still runs as fallback.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "first_exc",
    [
        RuntimeError("Event loop is closed"),
        ReferenceError("weakly-referenced object no longer exists"),
    ],
)
def test_do_close_absorbs_runtime_or_reference_from_first_close(
    first_exc: BaseException,
) -> None:
    """First ``close()`` raises RuntimeError/ReferenceError; ``do_close``
    returns None and the fallback runs."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc

    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()


@pytest.mark.parametrize(
    "first_exc",
    [
        RuntimeError("Event loop is closed"),
        ReferenceError("dead proxy"),
    ],
)
def test_async_dialect_do_close_absorbs_runtime_or_reference_from_first_close(
    first_exc: BaseException,
) -> None:
    """Async dialect inherits ``do_close``; the wider first-arm tuple applies."""
    dialect = DqliteDialect_aio()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc

    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()


def test_do_close_first_arm_still_propagates_programmer_bug() -> None:
    """``AttributeError`` from the first close is NOT absorbed."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = AttributeError("refactor bug")

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)


def test_do_close_first_arm_still_propagates_type_error() -> None:
    """Symmetric negative twin for ``TypeError``."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = TypeError("type bug")

    with pytest.raises(TypeError, match="type bug"):
        dialect.do_close(mock_conn)
