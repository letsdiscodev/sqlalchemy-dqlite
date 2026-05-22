"""Pin: ``AsyncAdaptedConnection._handle_exception`` splits out
``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit`` children
from a ``BaseExceptionGroup`` and re-raises them rather than
re-raising the raw group.

Without the split, SA's downstream ``isinstance(e, dbapi.Error)``
gate misses the group entirely — the pool slot stays live and the
raw group propagates to the user (a non-PEP-249 surface). Mirrors
the dbapi-layer ``_call_client`` cancel-class discipline so the
discipline is consistent end-to-end (cursor.py +
connection.py × 2 + SA aio.py).
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    """Construct a minimal AsyncAdaptedConnection for _handle_exception
    testing. The hook does not consult ``self._connection`` so we can
    drive it on a synthetic adapter.
    """
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    return adapter


def test_handle_exception_cancel_only_group_propagates_as_group() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup("cancel-only", [asyncio.CancelledError()])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)
    # ``raise cancel_group from None`` suppresses the implicit
    # ``__context__`` chain so the cancel forwarded to the caller's
    # structured-concurrency parent is unweighted by the original
    # ``BaseExceptionGroup`` that triggered the split. Mirrors the
    # dbapi-layer cursor.py / connection.py cancel-class arms.
    assert excinfo.value.__context__ is None
    assert excinfo.value.__cause__ is None


def test_handle_exception_mixed_group_propagates_cancel_partition() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "mixed",
        [asyncio.CancelledError(), OperationalError("oops", code=1)],
    )
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)


def test_handle_exception_pure_exception_group_wraps_as_operationalerror() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "transport-multi",
        [OperationalError("a", code=1), OperationalError("b", code=1)],
    )
    with pytest.raises(OperationalError) as excinfo:
        adapter._handle_exception(eg)
    # Remainder is accessible on __cause__ for SA's walk_cause_chain.
    assert isinstance(excinfo.value.__cause__, BaseExceptionGroup)


def test_handle_exception_keyboard_interrupt_group_propagates() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup("ki", [KeyboardInterrupt()])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, KeyboardInterrupt) for c in inner)


def test_handle_exception_systemexit_group_propagates() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup("se", [SystemExit(1)])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, SystemExit) for c in inner)


def test_handle_exception_non_group_passes_through_unchanged() -> None:
    """Non-BaseExceptionGroup errors take the existing ``raise error``
    path — the split is added without disturbing the prior behaviour.
    """
    adapter = _make_adapter()
    oe = OperationalError("plain", code=1)
    with pytest.raises(OperationalError) as excinfo:
        adapter._handle_exception(oe)
    assert excinfo.value is oe
