"""Pin: N concurrent BEGIN→SELECT→INSERT→COMMIT writers commit without retry.

The dbapi rewrites bare ``BEGIN`` to ``BEGIN IMMEDIATE`` so the writer-lock is
taken at BEGIN time, turning the unrecoverable SQLITE_BUSY_SNAPSHOT (517) race
into ordinary SQLITE_BUSY (5) that the busy_timeout retry absorbs.
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
