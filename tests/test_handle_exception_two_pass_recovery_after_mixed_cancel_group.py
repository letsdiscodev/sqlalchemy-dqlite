"""Pin: the cancellation-precedence trade-off in
``_handle_exception``'s cancel-class arm preserves the cancel-
propagation contract by discarding the ``remainder`` (the
non-cancel children of a mixed BaseExceptionGroup). The
loop-state RuntimeError is sacrificed for structured-concurrency.
Slot self-heal happens on the next acquire, when the same
underlying loop-state hop re-fires WITHOUT a sibling CancelledError
— ``_handle_exception`` then has only the loop-state member,
routes it through the remap, and the resulting
``OperationalError`` trips disconnect classification.

This test pins both halves of the contract:
1. First pass: mixed group → cancel propagates; loop-state lost.
2. Second pass: pure loop-state group → OperationalError, which
   then classifies as disconnect via SA's ``is_disconnect``.

Net cost: one wasted retry per cross-loop-cancel race.
"""

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

    # PASS 1: mixed group (cancel + loop-state). Cancellation
    # precedence: the cancel propagates; the loop-state remainder
    # is discarded (NOT routed through the remap).
    eg = BaseExceptionGroup(
        "mixed",
        [
            asyncio.CancelledError("user cancel"),
            RuntimeError("got Future <Future pending> attached to a different loop"),
        ],
    )
    with pytest.raises(BaseExceptionGroup) as first_excinfo:
        adapter._handle_exception(eg)
    # Only the cancel partition propagates; remainder is dropped.
    inner = first_excinfo.value.exceptions
    assert all(isinstance(c, asyncio.CancelledError) for c in inner), (
        "first pass must propagate cancel-only partition; the "
        "loop-state RuntimeError is intentionally discarded"
    )

    # PASS 2: the slot's next operation refires the same underlying
    # loop-state hop, this time WITHOUT a sibling cancel.
    # ``_handle_exception`` now routes it through the remap, surfacing
    # as ``OperationalError`` carrying the canonical
    # ``event-loop mismatch:`` / ``different event loop`` substrings.
    pure_loop_state_eg = BaseExceptionGroup(
        "pure-loop-state",
        [RuntimeError("got Future <Future pending> attached to a different loop")],
    )
    with pytest.raises(OperationalError) as second_excinfo:
        adapter._handle_exception(pure_loop_state_eg)

    # The OperationalError from the second pass MUST classify as
    # disconnect so SA's pool invalidates the slot.
    assert dialect.is_disconnect(second_excinfo.value, None, None) is True, (
        "second pass's OperationalError must be classified as disconnect so the slot is invalidated"
    )
