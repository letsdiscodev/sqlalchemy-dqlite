"""``pool_pre_ping=True`` end-to-end: a leader flip invalidates a
pooled slot and the next checkout reconnects transparently.

``do_ping`` is unit-tested for happy-path / closed-cursor /
programming-error / close-narrow paths. The integration contract —
"after a leader flip kills a pooled slot, ``pool_pre_ping`` causes
the next checkout to invalidate the dead slot and return a fresh
connection bound to the new leader without surfacing
'connection is closed' to the caller's first statement" — is
pinned here against the live cluster.

Both sub-tests use ``cluster_control.force_leader_flip()`` (from
``python-dqlite-dev/testlib``, bootstrapped in the top-level
``tests/conftest.py``) to mutate cluster topology mid-test, then
restore the original leader on the way out so subsequent tests see
a deterministic starting state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from dqlitetestlib import TestClusterControl  # type: ignore[import-not-found]


@pytest.mark.integration
def test_sync_pool_pre_ping_recovers_after_leader_flip(
    engine_url: str,
    cluster_control: TestClusterControl,
) -> None:
    """Sync engine with ``pool_pre_ping=True``: warm the pool, force
    a leader flip, the next checkout reconnects transparently and
    the user-visible ``SELECT 1`` succeeds first try.

    Without ``pool_pre_ping=True`` the pooled slot would be returned
    to the caller as-is and the user's first statement would surface
    a leader-change ``OperationalError``. The pin here is "with
    pre-ping, the failure is absorbed at checkout."
    """
    import asyncio
    import contextlib

    starting = asyncio.run(_starting_leader_node_id(cluster_control))

    engine = create_engine(engine_url, pool_pre_ping=True, pool_size=2, max_overflow=0)
    try:
        # Warm the pool — this checkout populates a slot that the
        # next check-out / pre-ping cycle will validate against.
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1

        # Force a leader flip. The pooled connection is now bound to
        # the stepped-down leader; pre-ping should detect that on
        # the next checkout and rebuild against the new leader.
        flip = asyncio.run(cluster_control.force_leader_flip())
        assert flip.target.node_id != starting

        # Caller-visible operation succeeds first try — pre-ping
        # invalidated the dead slot transparently.
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        with contextlib.suppress(Exception):
            asyncio.run(cluster_control.transfer_leadership_to(starting))
        engine.dispose()


@pytest.mark.integration
async def test_async_pool_pre_ping_recovers_after_leader_flip(
    async_engine_url: str,
    cluster_control: TestClusterControl,
) -> None:
    """Async engine with ``pool_pre_ping=True``: same contract as
    the sync sibling."""
    import contextlib

    starting_node = await cluster_control.current_leader_node()

    engine = create_async_engine(async_engine_url, pool_pre_ping=True, pool_size=2, max_overflow=0)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            assert result.scalar() == 1

        flip = await cluster_control.force_leader_flip()
        assert flip.target.node_id != starting_node.node_id

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
    finally:
        with contextlib.suppress(Exception):
            await cluster_control.transfer_leadership_to(starting_node.node_id)
        await engine.dispose()


async def _starting_leader_node_id(cluster_control: TestClusterControl) -> int:
    """Tiny helper so the sync test can capture the starting leader
    via a single ``asyncio.run`` call without duplicating the
    ``current_leader_node`` invocation pattern."""
    node = await cluster_control.current_leader_node()
    # ``NodeInfo.node_id`` is annotated ``int`` in the wire layer
    # but the testlib re-export does not propagate the type to
    # mypy via the dynamic sys.path bootstrap. Cast for clarity.
    return int(node.node_id)
