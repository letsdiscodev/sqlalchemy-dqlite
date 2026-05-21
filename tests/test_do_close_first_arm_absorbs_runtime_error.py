"""Pin: ``DqliteDialect.do_close``'s first-close ``except`` arm uses
``_FORCE_CLOSE_TAIL_EXCEPTIONS`` (the wider tuple), not the narrower
``_TRANSPORT_CLASS_EXCEPTIONS``.

Both ``RuntimeError("Event loop is closed")`` and ``ReferenceError``
on a dead inner proxy are documented (at ``_FORCE_CLOSE_TAIL_EXCEPTIONS``)
as reachable from the dbapi's own ``close()`` machinery during a
cross-loop ``engine.dispose()``. The earlier first-arm tuple omitted
both, so those raises escaped ``do_close`` and aborted SA's pool
finalize. With the wider tuple on the first arm the
"do_close never raises" invariant is honoured on the first-close
path too, and ``force_close_transport`` still runs as the fallback.
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
    """The first ``close()`` itself raises ``RuntimeError`` /
    ``ReferenceError``; ``do_close`` must return None AND the
    fallback ``force_close_transport`` must run."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc

    # Must NOT raise.
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
    """Async dialect inherits ``do_close``; the wider first-arm
    tuple applies symmetrically."""
    dialect = DqliteDialect_aio()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = first_exc

    dialect.do_close(mock_conn)

    mock_conn.force_close_transport.assert_called_once()


def test_do_close_first_arm_still_propagates_programmer_bug() -> None:
    """Negative twin: ``AttributeError`` from the first close
    (refactor regression) is NOT absorbed — the wider tuple still
    excludes programmer-bug classes so a real defect surfaces."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = AttributeError("refactor bug")

    with pytest.raises(AttributeError, match="refactor bug"):
        dialect.do_close(mock_conn)


def test_do_close_first_arm_still_propagates_type_error() -> None:
    """Symmetric negative twin for ``TypeError`` (programmer-bug
    class outside ``_FORCE_CLOSE_TAIL_EXCEPTIONS``)."""
    dialect = DqliteDialect()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = TypeError("type bug")

    with pytest.raises(TypeError, match="type bug"):
        dialect.do_close(mock_conn)
