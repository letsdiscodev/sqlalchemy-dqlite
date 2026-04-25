"""``pool_pre_ping=True`` end-to-end: a leader flip invalidates a
pooled slot transparently and the next checkout reconnects.

``do_ping`` is unit-tested for happy-path / closed-cursor / programming-
error / close-narrow paths. The integration contract — "after a
leader flip kills a pooled slot, ``pool_pre_ping`` causes the next
checkout to invalidate the dead slot and return a fresh connection
without surfacing 'connection is closed' to the caller's first
statement" — is unverified end-to-end.

Both sub-tests are xfail-gated on the same fixture work as tx-040:

1. The pool's leader-find chases container-internal addresses
   (``0.0.0.0:9001``) unreachable from the docker-host test runner.

2. There is no leader-flip primitive in the test fixtures.

Pin the test shape now so the moment the cluster fixture provides
both reachable redirect addresses and a leader-flip primitive, the
xfail marker can be removed.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
@pytest.mark.skip(
    reason=(
        "Gated on a leader-flip primitive in the test fixtures: "
        "without an actual mid-checkout flip, this test passes "
        "trivially (two SELECT 1 round-trips on the same warm pool) "
        "and provides no real coverage of the pool_pre_ping recovery "
        "path. Pinned for unblocking once the cluster fixture exposes "
        "force_leader_flip() (or equivalent)."
    )
)
def test_sync_pool_pre_ping_recovers_after_leader_flip(
    engine_url: str,
) -> None:
    """Engine with pool_pre_ping=True: warm the pool, force a
    leader flip, the next checkout reconnects transparently and the
    user-visible operation succeeds first try."""
    engine = create_engine(engine_url, pool_pre_ping=True, pool_size=2, max_overflow=0)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        # cluster.force_leader_flip() — yet-to-exist primitive
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        engine.dispose()


@pytest.mark.integration
@pytest.mark.skip(reason=("Gated on a leader-flip primitive; same as the sync variant above."))
async def test_async_pool_pre_ping_recovers_after_leader_flip(
    async_engine_url: str,
) -> None:
    engine = create_async_engine(async_engine_url, pool_pre_ping=True, pool_size=2, max_overflow=0)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        # cluster.force_leader_flip() — yet-to-exist primitive
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
    finally:
        await engine.dispose()
