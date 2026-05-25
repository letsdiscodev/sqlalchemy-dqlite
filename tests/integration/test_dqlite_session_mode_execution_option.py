"""Pin: the SA execution-option ``dqlite_session_mode`` controls the
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
    __tablename__ = "_session_mode_pin_usage"
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
    """``dqlite_session_mode="deferred"`` opts the engine out of the
    writer-safe rewrite. The SNAPSHOT race comes back — most
    concurrent writers BUSY-fail. Pin so the opt-out's behavioural
    contract is the documented "legacy semantics, you are on your
    own" not "still writer-safe somehow." """
    engine = create_async_engine(async_engine_url).execution_options(dqlite_session_mode="deferred")
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
    """``dqlite_session_mode="exclusive"`` serialises writes via the
    explicit ``BEGIN EXCLUSIVE`` literal. All writers commit
    transparently — the busy_timeout retry absorbs lock contention
    the same way it does for the default ``"immediate"`` path."""
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
    """Unknown ``dqlite_session_mode`` value raises ``ArgumentError`` at
    first connection checkout — the SA
    ``ConnectionCharacteristic.set_characteristic`` hook validates the
    value and refuses anything outside the accepted set. The misuse
    still surfaces config-time-ish (first ``engine.connect()``) rather
    than at the first ``do_begin`` against a leased connection."""
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
    """``dqlite_session_mode="read_only"`` emits ``PRAGMA query_only = 1``
    so the engine rejects every write at PREPARE with SQLITE_READONLY
    (primary code 8). The SA layer surfaces it as ``OperationalError``."""
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError

    # Schema bootstrap MUST run on a writer-mode engine — the read-only
    # engine cannot CREATE TABLE.
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
        # Reads succeed.
        async with ro_engine.connect() as conn:
            result = await conn.execute(select(_Usage).limit(1))
            assert result.fetchall() == []
        # Writes raise.
        with pytest.raises(OperationalError):
            async with ro_engine.begin() as conn:
                await conn.execute(text("INSERT INTO _session_mode_pin_usage (note) VALUES ('x')"))
    finally:
        await ro_engine.dispose()


@pytest.mark.asyncio
async def test_pool_checkin_restores_default_session_mode(
    async_engine_url: str,
) -> None:
    """After a SA Connection sets ``dqlite_session_mode="read_only"``
    via ``connection.execution_options`` and is closed, the next
    checkout from the pool sees the construction-time default
    (``"immediate"``) — the characteristic's
    ``reset_characteristic`` finalize-callback restores it on checkin
    so writes succeed again on the recycled slot."""
    from sqlalchemy import text

    engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)
            await conn.run_sync(_Base.metadata.create_all)

        # First checkout: opt this SA Connection into read_only.
        async with engine.connect() as conn:
            ro_conn = await conn.execution_options(dqlite_session_mode="read_only")
            # Writes must fail here.
            from sqlalchemy.exc import OperationalError

            with pytest.raises(OperationalError):
                async with ro_conn.begin():
                    await ro_conn.execute(
                        text("INSERT INTO _session_mode_pin_usage (note) VALUES ('a')")
                    )

        # Second checkout: same pooled slot (pool_size=1), but the
        # characteristic's reset_characteristic ran on checkin so the
        # slot is back to immediate (writer-safe). Writes must succeed.
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO _session_mode_pin_usage (note) VALUES ('b')"))
    finally:
        await engine.dispose()


def test_url_form_rejects_invalid_session_mode(async_engine_url: str) -> None:
    """``?session_mode=…`` URL parsing surfaces typos at
    ``create_engine`` time. The URL-query validator at
    ``base._URL_QUERY_ALLOWED`` runs synchronously during dialect
    URL parsing — no need to actually open a connection."""
    # The bad URL must surface from ``create_engine`` itself: the URL
    # validator is invoked by the dialect's ``create_connect_args``,
    # which engine bootstrapping calls eagerly.
    bad_url = async_engine_url + ("&" if "?" in async_engine_url else "?") + "session_mode=weak"
    with pytest.raises(ArgumentError):
        create_async_engine(bad_url)


@pytest.mark.asyncio
async def test_mid_transaction_toggle_raises_invalid_request_error(
    async_engine_url: str,
) -> None:
    """``transactional = True`` on the characteristic means
    ``execution_options(dqlite_session_mode=…)`` inside an open
    transaction raises ``InvalidRequestError`` (SA's standard guard
    in ``_set_connection_characteristics``). Matches PG's
    ``postgresql_readonly`` mid-tx behaviour."""
    from sqlalchemy.exc import InvalidRequestError

    engine = create_async_engine(async_engine_url)
    try:
        async with engine.connect() as conn, conn.begin():
            with pytest.raises(InvalidRequestError):
                await conn.execution_options(dqlite_session_mode="deferred")
    finally:
        await engine.dispose()
