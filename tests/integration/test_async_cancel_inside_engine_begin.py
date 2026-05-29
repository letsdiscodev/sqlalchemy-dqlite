"""Cancellation inside ``async with engine.begin()`` rolls back, no leak.

A CancelledError mid-block must ROLLBACK (not COMMIT), return the connection
to the pool, and propagate unchanged. Cancellation-during-COMMIT is covered
in ``test_async_cancel_during_commit_invalidates_slot.py``.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
class TestAsyncCancelInsideEngineBegin:
    async def test_cancel_inside_engine_begin_rolls_back(self, async_engine_url: str) -> None:
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_cancel_rb"))
                await conn.execute(text("CREATE TABLE async_cancel_rb (id INTEGER PRIMARY KEY)"))

            with pytest.raises(asyncio.TimeoutError):
                async with asyncio.timeout(0.1):
                    async with engine.begin() as conn:
                        await conn.execute(text("INSERT INTO async_cancel_rb (id) VALUES (1)"))
                        # Cancellation lands here.
                        await asyncio.sleep(1.0)
                        await conn.execute(text("INSERT INTO async_cancel_rb (id) VALUES (2)"))

            # Implicit ROLLBACK must have undone the cancelled row 1.
            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM async_cancel_rb"))).all()
            assert rows == []
        finally:
            await engine.dispose()

    async def test_repeated_cancellations_do_not_leak_pool_slots(
        self, async_engine_url: str
    ) -> None:
        """Repeated mid-transaction cancellations don't leak pool slots."""
        engine = create_async_engine(async_engine_url, pool_size=2, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            for _ in range(5):
                with pytest.raises(asyncio.TimeoutError):
                    async with asyncio.timeout(0.05):
                        async with engine.begin() as conn:
                            await conn.execute(text("SELECT 1"))
                            await asyncio.sleep(1.0)

            pool = engine.pool
            checkedout = pool.checkedout()  # type: ignore[attr-defined]
            assert checkedout == 0, (
                f"pool slots leaked: {checkedout} still checked out after cancellation cycles"
            )
        finally:
            await engine.dispose()
