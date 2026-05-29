"""Pin: ``do_begin``'s post-BEGIN ``cursor.close()`` uses the wider
``_FORCE_CLOSE_TAIL_EXCEPTIONS`` tuple (symmetric with ``do_close``), so cross-loop
``RuntimeError`` / dead-proxy ``ReferenceError`` can't escape the finally and mask the BEGIN
fault. Programmer-bug shapes (``AttributeError``/``TypeError``) stay outside and propagate."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def _make_dialect_and_conn(
    *,
    begin_exc: BaseException | None,
    close_exc: BaseException | None,
) -> tuple[DqliteDialect, MagicMock]:
    dialect = DqliteDialect()
    conn: Any = MagicMock()
    cursor = MagicMock()
    if begin_exc is not None:
        cursor.execute.side_effect = begin_exc
    if close_exc is not None:
        cursor.close.side_effect = close_exc
    conn.cursor.return_value = cursor
    return dialect, conn


def test_do_begin_preserves_operational_error_through_runtime_error_close() -> None:
    """A close-time ``RuntimeError("Event loop is closed")`` must not mask the BEGIN-time
    ``OperationalError``."""
    dialect, conn = _make_dialect_and_conn(
        begin_exc=OperationalError("real begin error"),
        close_exc=RuntimeError("Event loop is closed"),
    )

    with pytest.raises(OperationalError, match="real begin error"):
        dialect.do_begin(conn)


def test_do_begin_preserves_operational_error_through_reference_error_close() -> None:
    """Symmetric: a close-time ``ReferenceError`` from a dead ``weakref.proxy`` must not mask
    the BEGIN exception."""
    dialect, conn = _make_dialect_and_conn(
        begin_exc=OperationalError("real begin error"),
        close_exc=ReferenceError("weakly-referenced object no longer exists"),
    )

    with pytest.raises(OperationalError, match="real begin error"):
        dialect.do_begin(conn)


def test_do_begin_lets_attribute_error_close_propagate() -> None:
    """Programmer-bug ``AttributeError`` from ``cursor.close()`` is outside the wide tuple and
    must escape so the bug surfaces (BEGIN succeeds here)."""
    dialect, conn = _make_dialect_and_conn(
        begin_exc=None,
        close_exc=AttributeError("cursor lost its close hook"),
    )

    with pytest.raises(AttributeError, match="cursor lost its close hook"):
        dialect.do_begin(conn)
