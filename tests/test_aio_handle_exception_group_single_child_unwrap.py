"""``_handle_exception`` unwraps a single-child BaseExceptionGroup so the
original class identity survives (a group-of-one from a TaskGroup running
one task); otherwise an inner IntegrityError is rewritten to
OperationalError and ``except IntegrityError`` clauses miss it.
Multi-child remainders keep the aggregate wrap."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import IntegrityError, OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner: Any = MagicMock()
    inner.address = "localhost:9001"
    adapter._connection = inner
    return adapter


def test_single_child_integrity_error_group_unwraps() -> None:
    """A single-child group is unwrapped so the IntegrityError surfaces
    with its class identity and ``code`` intact."""
    adapter = _make_adapter()
    inner = IntegrityError("UNIQUE constraint failed", code=1555)
    group = BaseExceptionGroup("g", [inner])

    with pytest.raises(IntegrityError) as exc_info:
        adapter._handle_exception(group)

    raised = exc_info.value
    assert raised is inner, "must re-raise the original IntegrityError instance"
    assert getattr(raised, "code", None) == 1555, "code attribute must be preserved"
    # Remainder group is on __cause__ for SA's _walk_cause_chain; split
    # returns a fresh group, so assert by shape not identity.
    assert isinstance(raised.__cause__, BaseExceptionGroup)
    cause_group = raised.__cause__
    assert len(cause_group.exceptions) == 1
    assert cause_group.exceptions[0] is inner


def test_multi_child_group_still_wraps_as_operational_error() -> None:
    """A multi-child group has no single class to preserve, so it keeps
    the aggregate wrap."""
    adapter = _make_adapter()
    group = BaseExceptionGroup(
        "g",
        [IntegrityError("dup"), OperationalError("transient")],
    )

    with pytest.raises(OperationalError) as exc_info:
        adapter._handle_exception(group)

    msg = str(exc_info.value)
    assert "aggregate" in msg
    assert "IntegrityError" in msg
    assert "OperationalError" in msg
