"""SA's default ``pool_reset_on_return="rollback"`` actually rolls
back uncommitted transactions on connection return.

SA's pool default reset behaviour is "rollback" — the dialect
inherits this, and the underlying dbapi's ``rollback()`` is a
silent no-op when no transaction is active. The contract that
"an uncommitted INSERT vanishes after pool return" is unverified
end-to-end. A regression in the reset path becoming a no-op would
silently commit uncommitted writes for the next user.

Pin the contract for both sync and async engines. The test relies
on SA's auto-begin-on-first-execute behaviour: ``engine.connect()``
returns a connection in "no-tx" state; the first ``execute()`` of
DML triggers SA's internal transaction-begun state, which calls
``do_begin`` over the wire. Exit without commit/rollback hands the
connection back to the pool with an open transaction; the pool's
reset issues ROLLBACK and the row vanishes.
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

            # Uncommitted INSERT under SA's auto-begin transaction;
            # exit without commit/rollback so the pool reset path
            # has work to do.
            with engine.connect() as conn:
                conn.execute(text("INSERT INTO sync_pool_reset (id) VALUES (1)"))
                # Exit context without commit/rollback; the pool's
                # reset MUST roll back.

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
