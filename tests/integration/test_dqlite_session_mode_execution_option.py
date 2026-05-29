"""Pin: the ``dqlite_session_mode`` execution-option controls the BEGIN
literal the dialect emits, and an unknown mode raises ArgumentError."""

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
    __tablename__ = "_session_mode_pin_usage"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created = Column(DateTime)
    note = Column(String)


async def _drive_concurrent_writers(engine: object) -> list[bool]:
    """Run N_WRITERS concurrent SELECT-then-INSERT sessions; return per-writer success."""
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
    """``"deferred"`` opts out of the writer-safe rewrite, so the SNAPSHOT
    race returns and some writers BUSY-fail (legacy "you are on your own")."""
    engine = create_async_engine(async_engine_url).execution_options(dqlite_session_mode="deferred")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        ok = await _drive_concurrent_writers(engine)
        # No exact ratio: raft latency varies the count; only pin that the
        # opt-out is genuinely less safe than the default.
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
    """``"exclusive"`` serialises writes via ``BEGIN EXCLUSIVE``; all writers
    commit transparently like the default ``"immediate"`` path."""
    engine = create_async_engine(async_engine_url).execution_options(
        dqlite_session_mode="exclusive"
    )
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
    """``dqlite_session_mode`` accepts uppercase too (case-insensitive)."""
    engine = create_async_engine(async_engine_url).execution_options(
        dqlite_session_mode="IMMEDIATE"
    )
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
    """Unknown mode raises ArgumentError at first ``engine.connect()`` (the
    set_characteristic hook validates), not deferred to the first do_begin."""
    engine = create_async_engine(async_engine_url).execution_options(dqlite_session_mode="weak")
    try:
        with pytest.raises(ArgumentError, match="Invalid dqlite_session_mode"):
            async with engine.connect():
                pass
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_read_only_mode_rejects_writes_at_prepare(
    async_engine_url: str,
) -> None:
    """``"read_only"`` emits ``PRAGMA query_only = 1``; writes are rejected at
    PREPARE with SQLITE_READONLY (8), surfaced as OperationalError."""
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError

    # Schema bootstrap MUST run on a writer-mode engine — read-only can't CREATE.
    schema_engine = create_async_engine(async_engine_url)
    try:
        async with schema_engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)
    finally:
        await schema_engine.dispose()

    ro_engine = create_async_engine(async_engine_url).execution_options(
        dqlite_session_mode="read_only"
    )
    try:
        async with ro_engine.connect() as conn:
            result = await conn.execute(select(_Usage).limit(1))
            assert result.fetchall() == []
        with pytest.raises(OperationalError):
            async with ro_engine.begin() as conn:
                await conn.execute(text("INSERT INTO _session_mode_pin_usage (note) VALUES ('x')"))
    finally:
        await ro_engine.dispose()


@pytest.mark.asyncio
async def test_pool_checkin_restores_default_session_mode(
    async_engine_url: str,
) -> None:
    """reset_characteristic restores the default ``"immediate"`` on checkin, so a
    connection opted into read_only doesn't poison the next checkout of the slot."""
    from sqlalchemy import text

    engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        async with engine.connect() as conn:
            ro_conn = await conn.execution_options(dqlite_session_mode="read_only")
            from sqlalchemy.exc import OperationalError

            with pytest.raises(OperationalError):
                async with ro_conn.begin():
                    await ro_conn.execute(
                        text("INSERT INTO _session_mode_pin_usage (note) VALUES ('a')")
                    )

        # Same pooled slot (pool_size=1); reset_characteristic ran on checkin.
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO _session_mode_pin_usage (note) VALUES ('b')"))
    finally:
        await engine.dispose()


def test_url_form_rejects_invalid_session_mode(async_engine_url: str) -> None:
    """``?session_mode=…`` typos surface at ``create_engine`` time (URL validator
    runs synchronously during parsing); no connection needed."""
    bad_url = async_engine_url + ("&" if "?" in async_engine_url else "?") + "session_mode=weak"
    with pytest.raises(ArgumentError):
        create_async_engine(bad_url)


@pytest.mark.asyncio
async def test_mid_transaction_toggle_raises_invalid_request_error(
    async_engine_url: str,
) -> None:
    """transactional=True means setting the mode inside an open transaction
    raises InvalidRequestError, matching PG's ``postgresql_readonly``."""
    from sqlalchemy.exc import InvalidRequestError

    engine = create_async_engine(async_engine_url)
    try:
        async with engine.connect() as conn, conn.begin():
            with pytest.raises(InvalidRequestError):
                await conn.execution_options(dqlite_session_mode="deferred")
    finally:
        await engine.dispose()
