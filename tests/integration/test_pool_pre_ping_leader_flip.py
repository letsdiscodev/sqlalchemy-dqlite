"""``pool_pre_ping=True`` end-to-end: a leader flip invalidates a pooled slot
and the next checkout reconnects transparently against the live cluster.
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
    """Without pre-ping the stale slot would surface a leader-change
    OperationalError to the caller; pre-ping absorbs it at checkout."""
    import asyncio
    import contextlib

    starting = asyncio.run(_starting_leader_node_id(cluster_control))

    engine = create_engine(engine_url, pool_pre_ping=True, pool_size=2, max_overflow=0)
    try:
        # Warm the pool so the next checkout has a slot to pre-ping.
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1

        flip = asyncio.run(cluster_control.force_leader_flip())
        assert flip.target.node_id != starting

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
    """Async sibling of the sync pre-ping test."""
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
    node = await cluster_control.current_leader_node()
    # testlib's dynamic sys.path bootstrap loses NodeInfo.node_id's int
    # annotation for mypy; cast restores it.
    return int(node.node_id)
