"""``pool_reset_on_return="rollback"`` rolls back uncommitted writes on
connection return; a regression making the reset a no-op would silently
commit them for the next user.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
class TestPoolResetOnReturn:
    def test_sync_pool_reset_rollback_clears_uncommitted(self, engine_url: str) -> None:
        engine = create_engine(engine_url, pool_reset_on_return="rollback")
        try:
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sync_pool_reset"))
                conn.execute(text("CREATE TABLE sync_pool_reset (id INTEGER PRIMARY KEY)"))

            # Uncommitted INSERT under auto-begin; exit without
            # commit/rollback so the pool reset MUST roll it back.
            with engine.connect() as conn:
                conn.execute(text("INSERT INTO sync_pool_reset (id) VALUES (1)"))

            with engine.connect() as conn:
                rows = conn.execute(text("SELECT id FROM sync_pool_reset")).all()
            assert rows == []
        finally:
            engine.dispose()

    async def test_async_pool_reset_rollback_clears_uncommitted(
        self, async_engine_url: str
    ) -> None:
        engine = create_async_engine(async_engine_url, pool_reset_on_return="rollback")
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_pool_reset"))
                await conn.execute(text("CREATE TABLE async_pool_reset (id INTEGER PRIMARY KEY)"))

            async with engine.connect() as conn:
                await conn.execute(text("INSERT INTO async_pool_reset (id) VALUES (1)"))

            async with engine.connect() as conn:
                rows = (await conn.execute(text("SELECT id FROM async_pool_reset"))).all()
            assert rows == []
        finally:
            await engine.dispose()
