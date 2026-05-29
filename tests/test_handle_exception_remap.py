"""Pin: _handle_exception remaps the loop-mismatch RuntimeError to OperationalError.

SA gates is_disconnect on isinstance(e, dbapi.Error), so a bare RuntimeError would
leave the slot un-invalidated and the next checkout would hit the same fault."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection
from sqlalchemydqlite.base import DqliteDialect


def _make_adapter() -> AsyncAdaptedConnection:
    """Minimal instance bypassing the connect path; only _handle_exception is needed."""
    return AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)


def test_loop_mismatch_runtime_error_remapped_to_operational_error() -> None:
    adapter = _make_adapter()
    err = RuntimeError("<Future pending> attached to a different loop")
    with pytest.raises(OperationalError) as info:
        adapter._handle_exception(err)
    assert "different loop" in str(info.value)
    # Original RuntimeError preserved as __cause__ so debugging is intact.
    assert info.value.__cause__ is err


def test_short_different_loop_wording_also_remapped() -> None:
    """Pin both spellings: some asyncio versions omit the "attached to a" prefix."""
    adapter = _make_adapter()
    err = RuntimeError("Task got Future attached to a different loop")
    with pytest.raises(OperationalError):
        adapter._handle_exception(err)


def test_unrelated_runtime_error_not_remapped() -> None:
    """Negative pin: a generic RuntimeError passes through unchanged (identity preserved)."""
    adapter = _make_adapter()
    err = RuntimeError("some unrelated runtime fault")
    with pytest.raises(RuntimeError) as info:
        adapter._handle_exception(err)
    assert info.value is err


def test_other_exception_classes_passthrough_identity() -> None:
    adapter = _make_adapter()
    err = ValueError("test")
    with pytest.raises(ValueError) as info:
        adapter._handle_exception(err)
    assert info.value is err


def test_remapped_error_classified_as_disconnect_by_dialect() -> None:
    """End-to-end: the remapped OperationalError classifies as disconnect via substring."""
    adapter = _make_adapter()
    err = RuntimeError("Future attached to a different loop")
    try:
        adapter._handle_exception(err)
    except OperationalError as remapped:
        assert DqliteDialect().is_disconnect(remapped, None, None) is True
    else:  # pragma: no cover
        raise AssertionError("expected OperationalError")
