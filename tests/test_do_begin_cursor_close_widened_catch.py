"""Pin: ``do_begin``'s post-BEGIN ``cursor.close()`` catch is the
wider ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` tuple, symmetric with
``do_close``.

The narrower ``_TRANSPORT_CLASS_EXCEPTIONS`` previously left two
shapes — ``RuntimeError("Event loop is closed")`` from cross-loop
dispose and ``ReferenceError`` from a dead ``weakref.proxy`` — to
escape the ``finally`` body and mask the BEGIN-time exception per
Python's "finally replaces propagating exception" rule. SA's
exception wrapping then saw the wrong-class diagnostic and the
user observed a confusing "Event loop is closed" instead of the
real BEGIN-time fault.

Mirrors ``do_close``'s wider catch at the same surface (cursor /
connection close under cross-loop dispose / dead-proxy state).
Programmer-bug shapes (``AttributeError`` / ``TypeError``) and any
``IntegrityError`` from a custom audit trigger remain outside the
wide tuple and propagate as documented.
"""

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
    """``cursor.close()`` raising ``RuntimeError("Event loop is closed")``
    after a BEGIN that raised ``OperationalError`` must NOT mask the
    BEGIN exception. The wider catch swallows the RuntimeError so the
    OperationalError surfaces as the propagating exception."""
    dialect, conn = _make_dialect_and_conn(
        begin_exc=OperationalError("real begin error"),
        close_exc=RuntimeError("Event loop is closed"),
    )

    with pytest.raises(OperationalError, match="real begin error"):
        dialect.do_begin(conn)


def test_do_begin_preserves_operational_error_through_reference_error_close() -> None:
    """Symmetric: ``cursor.close()`` raising ``ReferenceError`` from a
    dead ``weakref.proxy`` post-BEGIN must NOT mask the BEGIN exception.
    """
    dialect, conn = _make_dialect_and_conn(
        begin_exc=OperationalError("real begin error"),
        close_exc=ReferenceError("weakly-referenced object no longer exists"),
    )

    with pytest.raises(OperationalError, match="real begin error"):
        dialect.do_begin(conn)


def test_do_begin_lets_attribute_error_close_propagate() -> None:
    """Programmer-bug ``AttributeError`` from ``cursor.close()`` is NOT
    in the wider tuple; it must escape so the bug surfaces. (BEGIN
    succeeds here so the close-time error is the only candidate.)"""
    dialect, conn = _make_dialect_and_conn(
        begin_exc=None,
        close_exc=AttributeError("cursor lost its close hook"),
    )

    with pytest.raises(AttributeError, match="cursor lost its close hook"):
        dialect.do_begin(conn)
