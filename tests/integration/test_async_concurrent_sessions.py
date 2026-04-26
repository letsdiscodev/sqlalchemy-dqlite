"""Two ``AsyncSession``s racing transactions on the same async engine.

The existing ``test_async_executemany.py`` runs workers sequentially
inside their own ``engine.begin()`` blocks. There is no test that
launches two ``AsyncSession`` transactions concurrently via
``asyncio.gather`` — the canonical "is the async dialect safe under
load?" smoke test.

Pin two contracts:

1. **Primary-key conflict**: two sessions racing INSERT on the same
   PK resolve via exactly one success and one failure. Under real
   transactional semantics the loser may surface either an
   ``IntegrityError`` (if its INSERT actually executes and trips
   the UNIQUE constraint) or an ``OperationalError("database is
   locked")`` (if it cannot acquire the writer lock before its
   sibling commits). Both shapes are valid losing-end states; the
   test asserts "exactly one survives", not the specific error
   class.

2. **Disjoint primary keys**: two sessions inserting different PKs
   serialise through the single-writer Raft channel and both
   eventually commit. The test runs them sequentially-in-time
   (without artificial overlap) so neither hits the writer-lock
   contention surface — the test is about pool slot independence
   and final state, not about contention semantics.
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
        """Two AsyncSessions race INSERT on the same primary key.
        Exactly one succeeds; the other gets either an
        ``IntegrityError`` (UNIQUE constraint trip) or an
        ``OperationalError`` (writer-lock contention). Both shapes
        are valid losing-end outcomes under dqlite's single-writer
        Raft channel + real transactions."""
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
            assert rows == [(1,)]
        finally:
            await engine.dispose()

    async def test_two_sessions_disjoint_keys_both_commit(self, async_engine_url: str) -> None:
        """Disjoint PKs: both sessions commit and final state has both
        rows. Confirms pool-slot independence and that the dialect can
        drive two concurrent sessions to a successful joint outcome.

        ``asyncio.gather`` runs the two writers concurrently; dqlite
        serialises writes through a single Raft writer lock, so a
        writer that tries to INSERT while a sibling is still inside
        its uncommitted transaction sees ``OperationalError("database
        is locked")``. That is the SQLite engine's documented response
        to writer contention — not a dqlite-specific quirk. Real
        applications respond by retrying with backoff (the same
        pattern stdlib ``sqlite3`` users follow when they hit
        SQLITE_BUSY). Mirror that here so the test pins "both commits
        eventually land" rather than "both commits land on the first
        try", which is not a contract dqlite (or stock SQLite) gives.
        """
        engine = create_async_engine(async_engine_url, pool_size=2, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_pk_disjoint"))
                await conn.execute(text("CREATE TABLE async_pk_disjoint (id INTEGER PRIMARY KEY)"))

            async def writer(key: int) -> None:
                # Bounded retry on writer-lock contention. Backoff
                # caps at ~1.6s total over 5 attempts — enough for the
                # sibling's tx to commit (typical commit latency is
                # sub-millisecond on a healthy local cluster) without
                # masking a genuine deadlock (which would never
                # resolve and would surface on the final attempt).
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
