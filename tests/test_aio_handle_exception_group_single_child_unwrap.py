"""Pin: ``AsyncAdaptedConnection._handle_exception`` unwraps a
single-child ``BaseExceptionGroup`` remainder so the original
exception class identity is preserved.

A PEP 654 / ``asyncio.TaskGroup`` / ``anyio.create_task_group``
caller that runs a SQL operation as a single inner task receives a
group-of-one wrapping the inner failure. Without the unwrap, an
``IntegrityError`` inside the group is silently rewritten to
``OperationalError(code=None, message="aggregate ...")`` — user
``except IntegrityError`` clauses miss the rewritten exception and
SA's ``is_disconnect`` substring scan runs on a message that isn't
a wire fault. The single-child unwrap is the structurally byte-
equivalent case to the non-group path; multi-child remainders keep
the aggregate wrap because no single class can be preserved.
"""

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
    """``BaseExceptionGroup("g", [IntegrityError(...)])`` is unwrapped
    so the IntegrityError surfaces with its original class identity
    and ``code`` attribute intact."""
    adapter = _make_adapter()
    inner = IntegrityError("UNIQUE constraint failed", code=1555)
    group = BaseExceptionGroup("g", [inner])

    with pytest.raises(IntegrityError) as exc_info:
        adapter._handle_exception(group)

    raised = exc_info.value
    assert raised is inner, "must re-raise the original IntegrityError instance"
    assert getattr(raised, "code", None) == 1555, "code attribute must be preserved"
    # The remainder group is preserved as __cause__ so SA's
    # _walk_cause_chain can still find structured-concurrency context
    # if needed. ``error.split`` returns a fresh group, not the
    # original — assert by class shape, not identity.
    assert isinstance(raised.__cause__, BaseExceptionGroup)
    cause_group = raised.__cause__
    assert len(cause_group.exceptions) == 1
    assert cause_group.exceptions[0] is inner


def test_multi_child_group_still_wraps_as_operational_error() -> None:
    """``BaseExceptionGroup("g", [IntegrityError, OperationalError])``
    has no single class to preserve; the aggregate wrap is the least-
    bad option."""
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
