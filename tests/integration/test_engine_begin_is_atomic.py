"""``engine.begin()`` blocks are atomic: rollback-on-exception undoes
all writes inside the block.

The historical behaviour (before ``do_begin`` was overridden) was that
no ``BEGIN`` was sent over the wire: every INSERT auto-committed at
the dqlite server, the subsequent ROLLBACK was a no-op, and partial
state from a failed transaction silently persisted. This was a
systemic violation of transactional atomicity affecting every
SA-managed write.

Pin the corrected contract from multiple angles:

- Sync and async ``engine.begin()`` rollback-on-exception undoes
  the rows.
- Multi-statement blocks roll back ALL writes, not just the last.
- Explicit ``connection.begin()`` (the lower-level analogue of
  ``engine.begin()``) has the same atomicity.
- ORM unit-of-work pattern (``session.flush()`` then raise then
  ``session.rollback()``) undoes the flushed writes — the canonical
  consequence the bug had on user-visible ORM behaviour.
- A wire-trace test confirms ``BEGIN`` is actually emitted before
  the first DML inside the block — the load-bearing assertion that
  the fix is in place.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

_Base = declarative_base()


class _AtomicRow(_Base):  # type: ignore[misc,valid-type]
    __tablename__ = "atomic_orm_rows"
    id = Column(Integer, primary_key=True)
    val = Column(String, nullable=True)


class _ForcedRollback(Exception):
    """Sentinel exception raised inside ``engine.begin()`` blocks to
    drive the rollback path."""


@pytest.mark.integration
class TestEngineBeginIsAtomic:
    def test_sync_engine_begin_rollback_undoes_writes(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        try:
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS atomic_sync"))
                conn.execute(text("CREATE TABLE atomic_sync (id INTEGER PRIMARY KEY)"))

            with pytest.raises(_ForcedRollback), engine.begin() as conn:
                conn.execute(text("INSERT INTO atomic_sync (id) VALUES (1)"))
                raise _ForcedRollback("force the rollback path")

            with engine.begin() as conn:
                rows = conn.execute(text("SELECT id FROM atomic_sync")).all()
            assert rows == []
        finally:
            engine.dispose()

    async def test_async_engine_begin_rollback_undoes_writes(self, async_engine_url: str) -> None:
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS atomic_async"))
                await conn.execute(text("CREATE TABLE atomic_async (id INTEGER PRIMARY KEY)"))

            with pytest.raises(_ForcedRollback):
                async with engine.begin() as conn:
                    await conn.execute(text("INSERT INTO atomic_async (id) VALUES (1)"))
                    raise _ForcedRollback("force the rollback path")

            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM atomic_async"))).all()
            assert rows == []
        finally:
            await engine.dispose()

    async def test_engine_begin_multi_insert_full_rollback(self, async_engine_url: str) -> None:
        """All N writes in a multi-statement block roll back together
        — the bug's worst surface was that some rows persisted while
        others did not, depending on whether the exception fired
        before or after a particular statement."""
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS atomic_multi"))
                await conn.execute(text("CREATE TABLE atomic_multi (id INTEGER PRIMARY KEY)"))

            with pytest.raises(_ForcedRollback):
                async with engine.begin() as conn:
                    for i in range(10):
                        await conn.execute(
                            text("INSERT INTO atomic_multi (id) VALUES (:i)"),
                            {"i": i},
                        )
                    raise _ForcedRollback("after 10 inserts")

            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM atomic_multi"))).all()
            assert rows == []
        finally:
            await engine.dispose()

    def test_connection_begin_rollback_undoes_writes(self, engine_url: str) -> None:
        """``engine.connect().begin()`` is the lower-level analogue of
        ``engine.begin()`` — both route through ``do_begin``."""
        engine = create_engine(engine_url)
        try:
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS atomic_conn_begin"))
                conn.execute(text("CREATE TABLE atomic_conn_begin (id INTEGER PRIMARY KEY)"))

            with engine.connect() as conn:
                tx = conn.begin()
                try:
                    conn.execute(text("INSERT INTO atomic_conn_begin (id) VALUES (1)"))
                    tx.rollback()
                except BaseException:
                    tx.rollback()
                    raise

            with engine.begin() as conn:
                rows = conn.execute(text("SELECT id FROM atomic_conn_begin")).all()
            assert rows == []
        finally:
            engine.dispose()

    async def test_engine_begin_ddl_rollback_undoes_create_table(
        self, async_engine_url: str
    ) -> None:
        """SQLite/dqlite does NOT auto-commit DDL (unlike some other
        databases). Inside a real ``engine.begin()`` a ``CREATE TABLE``
        followed by an exception MUST be rolled back. The bug
        previously hid this: every DDL auto-committed, leaving a
        half-finished schema state visible to the next caller.

        Pin: after the rollback, the table does not exist."""
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS atomic_ddl"))

            with pytest.raises(_ForcedRollback):
                async with engine.begin() as conn:
                    await conn.execute(text("CREATE TABLE atomic_ddl (id INTEGER PRIMARY KEY)"))
                    raise _ForcedRollback("after CREATE TABLE")

            # The CREATE TABLE was inside a real transaction that
            # rolled back; the table must NOT exist. Probe via
            # SELECT and expect OperationalError("no such table").
            from sqlalchemy.exc import OperationalError

            async with engine.connect() as conn:
                with pytest.raises(OperationalError, match="no such table"):
                    await conn.execute(text("SELECT id FROM atomic_ddl"))
        finally:
            await engine.dispose()

    async def test_async_connection_begin_rollback_undoes_writes(
        self, async_engine_url: str
    ) -> None:
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS atomic_async_conn_begin"))
                await conn.execute(
                    text("CREATE TABLE atomic_async_conn_begin (id INTEGER PRIMARY KEY)")
                )

            async with engine.connect() as conn:
                tx = await conn.begin()
                await conn.execute(text("INSERT INTO atomic_async_conn_begin (id) VALUES (1)"))
                await tx.rollback()

            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM atomic_async_conn_begin"))).all()
            assert rows == []
        finally:
            await engine.dispose()


@pytest.mark.integration
class TestSessionFlushRollbackUndoesWrites:
    """ORM unit-of-work pattern: the broken state of tx-064 silently
    committed flushed rows when the session was rolled back."""

    def test_sync_session_flush_then_rollback_undoes_writes(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        try:
            with engine.begin() as conn:
                _Base.metadata.drop_all(conn, tables=[_AtomicRow.__table__])
                _Base.metadata.create_all(conn, tables=[_AtomicRow.__table__])

            Session_ = sessionmaker(bind=engine, expire_on_commit=False)
            with pytest.raises(_ForcedRollback), Session_() as session:
                session.add(_AtomicRow(val="a"))
                session.add(_AtomicRow(val="b"))
                session.flush()
                raise _ForcedRollback("after flush")

            with Session_() as session:
                rows = session.query(_AtomicRow).all()
            assert rows == []
        finally:
            engine.dispose()

    async def test_async_session_flush_then_rollback_undoes_writes(
        self, async_engine_url: str
    ) -> None:
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(
                    lambda c: _Base.metadata.drop_all(c, tables=[_AtomicRow.__table__])
                )
                await conn.run_sync(
                    lambda c: _Base.metadata.create_all(c, tables=[_AtomicRow.__table__])
                )

            AsyncSession_ = async_sessionmaker(bind=engine, expire_on_commit=False)
            with pytest.raises(_ForcedRollback):
                async with AsyncSession_() as session:
                    session.add(_AtomicRow(val="a"))
                    session.add(_AtomicRow(val="b"))
                    await session.flush()
                    raise _ForcedRollback("after flush")

            async with AsyncSession_() as session:
                rows = (await session.execute(text("SELECT val FROM atomic_orm_rows"))).all()
            assert rows == []
        finally:
            await engine.dispose()


@pytest.mark.integration
class TestEngineBeginEmitsBeginOverWire:
    """Wire-trace pin: BEGIN must actually be sent before the first
    DML inside an ``engine.begin()`` block. This is the canonical
    "tx-064 fix is in place" assertion."""

    async def test_async_engine_begin_emits_begin_before_dml(self, async_engine_url: str) -> None:
        import dqliteclient.connection as cc

        recorded: list[str] = []
        orig_execute = cc.DqliteConnection.execute

        async def traced(self, sql, params=None):
            recorded.append(sql)
            return await orig_execute(self, sql, params)

        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS wire_trace"))
                await conn.execute(text("CREATE TABLE wire_trace (id INTEGER PRIMARY KEY)"))

            recorded.clear()
            cc.DqliteConnection.execute = traced
            try:
                async with engine.begin() as conn:
                    await conn.execute(text("INSERT INTO wire_trace (id) VALUES (1)"))
            finally:
                cc.DqliteConnection.execute = orig_execute

            # The trace MUST start with BEGIN; the INSERT MUST follow;
            # the COMMIT MUST close it. Other entries (pool reset
            # ROLLBACKs etc.) are tolerated.
            assert "BEGIN" in recorded, f"no BEGIN in trace: {recorded!r}"
            begin_idx = recorded.index("BEGIN")
            insert_idx = next(i for i, s in enumerate(recorded) if "INSERT INTO wire_trace" in s)
            commit_idx = recorded.index("COMMIT")
            assert begin_idx < insert_idx < commit_idx, f"unexpected order: {recorded!r}"
        finally:
            await engine.dispose()
            # Be paranoid about restoration in case of test infrastructure failure.
            cc.DqliteConnection.execute = orig_execute


# Suppress unused-import warning for asyncio (tests use it implicitly via pytest-asyncio).
_ = asyncio
