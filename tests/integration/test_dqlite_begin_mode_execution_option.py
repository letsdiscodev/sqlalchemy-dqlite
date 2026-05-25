"""Pin: the SA execution-option ``dqlite_begin_mode`` controls the
literal BEGIN form the dialect emits.

Default ``"immediate"`` (writer-safe — bare ``BEGIN`` rewritten to
``BEGIN IMMEDIATE`` by the dbapi cursor). ``"deferred"`` emits the
explicit literal which bypasses the dbapi rewrite — restores the
legacy DEFERRED semantics for explicitly read-only sessions that
want to avoid the writer-lock serialization tax. ``"exclusive"``
emits the explicit literal for stronger lock semantics.

The behavioural pin: with ``"immediate"`` (default), 8 concurrent
writers all commit transparently; with ``"deferred"``, the SNAPSHOT
race the dbapi rewrite was designed to eliminate comes back
(several writers BUSY-fail). The ``"exclusive"`` mode serialises
writes the same way ``"immediate"`` does and is also expected to
commit transparently.

Argument validation pin: an unknown mode raises ``ArgumentError``
at ``execution_options(...)`` time.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import Column, DateTime, Integer, String, select
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

N_WRITERS = 8


class _Base(DeclarativeBase):
    pass


class _Usage(_Base):
    __tablename__ = "_begin_mode_pin_usage"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created = Column(DateTime)
    note = Column(String)


async def _drive_concurrent_writers(engine: object) -> list[bool]:
    """Run ``N_WRITERS`` concurrent SELECT-then-INSERT sessions.
    Return a list of per-writer success booleans."""
    SessionMaker = pytest.importorskip("sqlalchemy.ext.asyncio").async_sessionmaker(
        engine, expire_on_commit=False
    )

    async def one(i: int) -> bool:
        try:
            async with SessionMaker.begin() as session:
                await session.execute(select(_Usage).limit(1))
                await session.execute(select(_Usage).where(_Usage.id == 1))
                session.add(_Usage(created=datetime.now(UTC), note=f"req-{i}"))
            return True
        except Exception:  # noqa: BLE001
            return False

    return await asyncio.gather(*(one(i) for i in range(N_WRITERS)))


@pytest.mark.asyncio
async def test_default_immediate_mode_all_writers_commit(
    async_engine_url: str,
) -> None:
    """No execution_options → default ``"immediate"`` → all writers commit."""
    engine = create_async_engine(async_engine_url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        ok = await _drive_concurrent_writers(engine)
        assert sum(ok) == N_WRITERS, (
            f"default mode (immediate) must commit all {N_WRITERS} concurrent "
            f"writers; got {sum(ok)}"
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_deferred_mode_restores_legacy_snapshot_race(
    async_engine_url: str,
) -> None:
    """``dqlite_begin_mode="deferred"`` opts the engine out of the
    writer-safe rewrite. The SNAPSHOT race comes back — most
    concurrent writers BUSY-fail. Pin so the opt-out's behavioural
    contract is the documented "legacy semantics, you are on your
    own" not "still writer-safe somehow." """
    engine = create_async_engine(async_engine_url).execution_options(dqlite_begin_mode="deferred")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        ok = await _drive_concurrent_writers(engine)
        # Expect at least some writers to fail under deferred
        # semantics — the SNAPSHOT race is structurally present.
        # We don't pin an exact ratio (the cluster's raft latency
        # affects the precise count); we pin that the opt-out is
        # genuinely less safe than the default.
        assert sum(ok) < N_WRITERS, (
            "deferred mode must surface the SNAPSHOT race on at "
            f"least one writer; got {sum(ok)}/{N_WRITERS} succeeded "
            "(was the dbapi rewrite still firing?)"
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_exclusive_mode_all_writers_commit(
    async_engine_url: str,
) -> None:
    """``dqlite_begin_mode="exclusive"`` serialises writes via the
    explicit ``BEGIN EXCLUSIVE`` literal. All writers commit
    transparently — the busy_timeout retry absorbs lock contention
    the same way it does for the default ``"immediate"`` path."""
    engine = create_async_engine(async_engine_url).execution_options(dqlite_begin_mode="exclusive")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        ok = await _drive_concurrent_writers(engine)
        assert sum(ok) == N_WRITERS, (
            f"exclusive mode must commit all {N_WRITERS} concurrent writers; got {sum(ok)}"
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_case_insensitive_mode_accepted(
    async_engine_url: str,
) -> None:
    """``dqlite_begin_mode`` accepts uppercase too (case-insensitive)."""
    engine = create_async_engine(async_engine_url).execution_options(dqlite_begin_mode="IMMEDIATE")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        ok = await _drive_concurrent_writers(engine)
        assert sum(ok) == N_WRITERS
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_invalid_mode_raises_argument_error(
    async_engine_url: str,
) -> None:
    """Unknown ``dqlite_begin_mode`` value raises at the
    ``execution_options`` boundary so the misuse surfaces at config
    time, not at the first ``do_begin``."""
    engine = create_async_engine(async_engine_url)
    try:
        with pytest.raises(ArgumentError, match="Invalid dqlite_begin_mode"):
            engine.execution_options(dqlite_begin_mode="weak")
    finally:
        await engine.dispose()
