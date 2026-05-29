"""Two ``AsyncSession``s racing transactions on the same async engine,
launched concurrently via ``asyncio.gather`` — the async-under-load smoke test.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


@pytest.mark.integration
class TestAsyncConcurrentSessions:
    async def test_two_sessions_pk_conflict_one_loses(self, async_engine_url: str) -> None:
        """Two sessions race INSERT on the same PK: exactly one succeeds; the
        loser gets IntegrityError (UNIQUE) or OperationalError (writer-lock)."""
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
            # Either contention failure mode is acceptable.
            assert isinstance(failures[0], (IntegrityError, OperationalError))

            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM async_pk_conflict"))).all()
            # SA Row's __eq__ with tuples isn't reflected in mypy's stubs.
            assert [tuple(r) for r in rows] == [(1,)]
        finally:
            await engine.dispose()

    async def test_two_sessions_disjoint_keys_both_commit(self, async_engine_url: str) -> None:
        """Disjoint PKs: both sessions commit. dqlite serialises writes via a
        single Raft writer lock, so a writer overlapping a sibling's tx sees
        ``database is locked`` (SQLITE_BUSY); retrying with backoff is the
        expected response, so the test pins "both eventually commit"."""
        engine = create_async_engine(async_engine_url, pool_size=2, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_pk_disjoint"))
                await conn.execute(text("CREATE TABLE async_pk_disjoint (id INTEGER PRIMARY KEY)"))

            async def writer(key: int) -> None:
                # Bounded retry on writer-lock contention (5 attempts, ~1.6s
                # total): enough for the sibling to commit, short enough that a
                # genuine deadlock still surfaces on the final attempt.
                last_exc: OperationalError | None = None
                backoff = 0.05
                for _ in range(5):
                    try:
                        async with AsyncSession(engine) as session, session.begin():
                            await session.execute(
                                text("INSERT INTO async_pk_disjoint (id) VALUES (:k)"),
                                {"k": key},
                            )
                        return
                    except OperationalError as exc:
                        if "database is locked" not in str(exc).lower():
                            raise
                        last_exc = exc
                        await asyncio.sleep(backoff)
                        backoff *= 2
                assert last_exc is not None
                raise last_exc

            await asyncio.gather(writer(1), writer(2))

            async with engine.begin() as conn:
                rows = sorted(
                    r[0]
                    for r in (await conn.execute(text("SELECT id FROM async_pk_disjoint"))).all()
                )
            assert rows == [1, 2]
        finally:
            await engine.dispose()
