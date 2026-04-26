"""Pin: ``AsyncAdaptedConnection._handle_exception`` remaps the
``await_only`` loop-mismatch ``RuntimeError`` into
``dbapi.OperationalError`` so the SA pool's ``is_disconnect``
classifies it and the slot is invalidated.

Without the remap, a bare ``RuntimeError`` escapes
commit/rollback/execute paths; SA gates ``is_disconnect`` on
``isinstance(e, dbapi.Error)`` so the slot is not invalidated and the
next checkout hits the same fault.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection
from sqlalchemydqlite.base import DqliteDialect


def _make_adapter() -> AsyncAdaptedConnection:
    """Build a minimal ``AsyncAdaptedConnection`` without going through
    the dialect's connect path — we only need ``_handle_exception``."""
    return AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)


def test_loop_mismatch_runtime_error_remapped_to_operational_error() -> None:
    """The exact wording asyncio emits when a future is awaited in
    the wrong loop must be remapped."""
    adapter = _make_adapter()
    err = RuntimeError("<Future pending> attached to a different loop")
    with pytest.raises(OperationalError) as info:
        adapter._handle_exception(err)
    assert "different loop" in str(info.value)
    # Original RuntimeError preserved as __cause__ so debugging is intact.
    assert info.value.__cause__ is err


def test_short_different_loop_wording_also_remapped() -> None:
    """Some asyncio versions emit just "different loop" without the
    "attached to a" prefix. Pin both spellings."""
    adapter = _make_adapter()
    err = RuntimeError("Task got Future attached to a different loop")
    with pytest.raises(OperationalError):
        adapter._handle_exception(err)


def test_unrelated_runtime_error_not_remapped() -> None:
    """Negative pin: a generic RuntimeError must NOT be remapped — the
    remap is for the loop-mismatch shape only. Identity passthrough
    must preserve the type so caller's existing handlers still match."""
    adapter = _make_adapter()
    err = RuntimeError("some unrelated runtime fault")
    with pytest.raises(RuntimeError) as info:
        adapter._handle_exception(err)
    assert info.value is err


def test_other_exception_classes_passthrough_identity() -> None:
    """Negative pin: ValueError, TypeError, etc. must passthrough."""
    adapter = _make_adapter()
    err = ValueError("test")
    with pytest.raises(ValueError) as info:
        adapter._handle_exception(err)
    assert info.value is err


def test_remapped_error_classified_as_disconnect_by_dialect() -> None:
    """End-to-end pin: the remapped ``OperationalError`` carries the
    "different loop" substring, and the dialect's substring fallback
    returns True from ``is_disconnect``."""
    adapter = _make_adapter()
    err = RuntimeError("Future attached to a different loop")
    try:
        adapter._handle_exception(err)
    except OperationalError as remapped:
        assert DqliteDialect().is_disconnect(remapped, None, None) is True
    else:  # pragma: no cover
        raise AssertionError("expected OperationalError")
