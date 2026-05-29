"""Pin the cancellation-precedence trade-off in _handle_exception's cancel arm.

A mixed BaseExceptionGroup propagates the cancel and discards the loop-state remainder
(structured-concurrency wins); the slot self-heals on the next acquire when the loop-state
hop re-fires alone and remaps to a disconnect OperationalError. Cost: one wasted retry."""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection
from sqlalchemydqlite.base import DqliteDialect


def _make_adapter() -> AsyncAdaptedConnection:
    return AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)


def test_mixed_cancel_group_self_heals_on_second_pass() -> None:
    adapter = _make_adapter()
    dialect = DqliteDialect()

    # PASS 1: mixed group — cancel propagates, loop-state remainder is discarded.
    eg = BaseExceptionGroup(
        "mixed",
        [
            asyncio.CancelledError("user cancel"),
            RuntimeError("got Future <Future pending> attached to a different loop"),
        ],
    )
    with pytest.raises(BaseExceptionGroup) as first_excinfo:
        adapter._handle_exception(eg)
    inner = first_excinfo.value.exceptions
    assert all(isinstance(c, asyncio.CancelledError) for c in inner), (
        "first pass must propagate cancel-only partition; the "
        "loop-state RuntimeError is intentionally discarded"
    )

    # PASS 2: same loop-state hop refires alone, now remapped to OperationalError.
    pure_loop_state_eg = BaseExceptionGroup(
        "pure-loop-state",
        [RuntimeError("got Future <Future pending> attached to a different loop")],
    )
    with pytest.raises(OperationalError) as second_excinfo:
        adapter._handle_exception(pure_loop_state_eg)

    assert dialect.is_disconnect(second_excinfo.value, None, None) is True, (
        "second pass's OperationalError must be classified as disconnect so the slot is invalidated"
    )
