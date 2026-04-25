"""Two ``AsyncSession``s racing transactions on the same async engine.

The existing ``test_async_executemany.py`` runs workers sequentially
inside their own ``engine.begin()`` blocks. There is no test that
launches two ``AsyncSession`` transactions concurrently via
``asyncio.gather`` — the canonical "is the async dialect safe under
load?" smoke test.

Pin two contracts:

1. **Primary-key conflict**: two sessions inserting the same PK
   must resolve via exactly one IntegrityError; the other commits.
   Confirms transaction isolation and pool slot independence.

2. **Disjoint primary keys**: two sessions inserting different PKs
   must both commit. Confirms the pool gives each session a slot
   and that one session's open transaction does not block the
   other's commit pathologically.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


@pytest.mark.integration
class TestAsyncConcurrentSessions:
    async def test_two_sessions_pk_conflict_one_loses(self, async_engine_url: str) -> None:
        """Two AsyncSessions race INSERT on the same primary key.
        Exactly one succeeds; the other gets IntegrityError."""
        engine = create_async_engine(async_engine_url, pool_size=2, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_pk_conflict"))
                await conn.execute(text("CREATE TABLE async_pk_conflict (id INTEGER PRIMARY KEY)"))

            async def writer() -> None:
                async with AsyncSession(engine) as session, session.begin():
                    await session.execute(text("INSERT INTO async_pk_conflict (id) VALUES (1)"))
                    # Hold the tx briefly so the second writer overlaps.
                    await asyncio.sleep(0.05)

            results = await asyncio.gather(writer(), writer(), return_exceptions=True)
            successes = [r for r in results if not isinstance(r, BaseException)]
            failures = [r for r in results if isinstance(r, BaseException)]

            assert len(successes) == 1
            assert len(failures) == 1
            assert isinstance(failures[0], IntegrityError)

            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM async_pk_conflict"))).all()
            assert rows == [(1,)]
        finally:
            await engine.dispose()

    async def test_two_sessions_disjoint_keys_both_commit(self, async_engine_url: str) -> None:
        """Disjoint PKs: both sessions commit; pool serves them
        independent slots."""
        engine = create_async_engine(async_engine_url, pool_size=2, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_pk_disjoint"))
                await conn.execute(text("CREATE TABLE async_pk_disjoint (id INTEGER PRIMARY KEY)"))

            async def writer(key: int) -> None:
                async with AsyncSession(engine) as session, session.begin():
                    await session.execute(
                        text("INSERT INTO async_pk_disjoint (id) VALUES (:k)"),
                        {"k": key},
                    )
                    await asyncio.sleep(0.05)

            await asyncio.gather(writer(1), writer(2))

            async with engine.begin() as conn:
                rows = sorted(
                    r[0]
                    for r in (await conn.execute(text("SELECT id FROM async_pk_disjoint"))).all()
                )
            assert rows == [1, 2]
        finally:
            await engine.dispose()
