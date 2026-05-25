"""Pin: SA's standard ``BEGIN → SELECT → INSERT → COMMIT`` pattern
survives N concurrent writers without caller-side retry.

Stdlib ``sqlite3 + aiosqlite + :memory:?cache=shared`` runs the same
workload without failures because shared-cache mode bypasses WAL /
MVCC entirely — concurrent writers serialize through the in-process
mutex. Against dqlite (and any real WAL backend) the SELECTs acquire
a read-snapshot that a concurrent committer can invalidate, and the
subsequent INSERT then surfaces ``SQLITE_BUSY_SNAPSHOT (517)`` —
unrecoverable without a transaction restart.

dqlite-server's own advice (``vfs.c:1579`` in dqlite-upstream)
recommends ``BEGIN IMMEDIATE`` for write-bearing transactions: it
acquires the writer-lock at BEGIN time so the SELECTs can't be
overtaken. The dbapi layer rewrites bare ``BEGIN`` (and explicit
``BEGIN DEFERRED`` / ``BEGIN TRANSACTION``) to ``BEGIN IMMEDIATE``
by default. Concurrent ``BEGIN IMMEDIATE`` calls contend at the
writer-lock and surface as ordinary ``SQLITE_BUSY (5)``, which the
existing busy_timeout retry absorbs transparently — matching what
stdlib SQLite does for its plain-BUSY path.

This integration test reproduces the user-reported failure (8
concurrent ``AsyncSession.begin()`` each doing two SELECTs + one
INSERT) and asserts all N writers commit without caller-side
retry. Pre-fix: 6/8 fail with SNAPSHOT; post-fix: 8/8 commit.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import Column, DateTime, Integer, String, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

# 8 mirrors the reproducer in the upstream bug report.
N_WRITERS = 8


class _Base(DeclarativeBase):
    pass


class _Usage(_Base):
    __tablename__ = "_begin_imm_usage_pin"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created = Column(DateTime)
    note = Column(String)


@pytest.mark.asyncio
async def test_concurrent_writers_default_begin_immediate_absorbs_contention(
    async_engine_url: str,
) -> None:
    """Pre-fix: 6/8 fail with SNAPSHOT. Post-fix: all 8 commit."""
    engine = create_async_engine(async_engine_url)
    SessionMaker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        async def one_request(i: int) -> None:
            async with SessionMaker.begin() as session:
                await session.execute(select(_Usage).limit(1))
                await session.execute(select(_Usage).where(_Usage.id == 1))
                session.add(
                    _Usage(
                        created=datetime.now(UTC),
                        note=f"req-{i}",
                    )
                )

        results = await asyncio.gather(
            *(one_request(i) for i in range(N_WRITERS)),
            return_exceptions=True,
        )
        failures = [r for r in results if isinstance(r, BaseException)]
        assert not failures, (
            f"{len(failures)}/{N_WRITERS} concurrent writers failed; "
            f"dbapi's BEGIN → BEGIN IMMEDIATE rewrite must absorb "
            f"single-writer Raft contention transparently. First: "
            f"{failures[0]!r}"
        )

        async with engine.begin() as conn:
            rows = (await conn.execute(select(_Usage.id).order_by(_Usage.id))).all()
        assert len(rows) == N_WRITERS
    finally:
        await engine.dispose()
