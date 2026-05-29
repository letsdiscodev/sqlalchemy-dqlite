"""Integration coverage for SA pool features that interact with the
dialect's ``do_close`` / ``do_terminate`` / ``do_ping`` hooks.
"""

from __future__ import annotations

import time

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
def test_sync_pool_recycle_calls_do_terminate(engine_url: str) -> None:
    """SA's recycle path must route through ``do_terminate``, not the
    graceful ``do_close``; a regression there would silently extend
    recycle latency."""
    eng = create_engine(engine_url, pool_recycle=0.1, pool_size=1, max_overflow=0)
    try:
        terminate_calls: list[object] = []
        original = eng.dialect.do_terminate

        def spy(dbapi_conn: object) -> None:
            terminate_calls.append(dbapi_conn)
            return original(dbapi_conn)  # type: ignore[arg-type]

        eng.dialect.do_terminate = spy  # type: ignore[assignment]

        with eng.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        time.sleep(0.2)  # past recycle window
        # Next checkout recycles the stale slot.
        with eng.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()

        assert len(terminate_calls) >= 1, (
            "pool_recycle did not invoke dialect.do_terminate on the "
            "stale slot — SA may have changed the recycle path's "
            "terminate=True flag, or the dialect's do_terminate hook "
            "is not wired up correctly."
        )
    finally:
        eng.dispose()


@pytest.mark.integration
def test_sync_engine_dispose_calls_do_close_per_slot(engine_url: str) -> None:
    """dispose() must route every queued slot through ``do_close`` (not
    ``do_terminate``). Uses ``deferred`` session mode so the 3 parallel
    connections don't serialize on the writer-lock via ``BEGIN IMMEDIATE``."""
    eng = create_engine(engine_url, pool_size=3, max_overflow=0).execution_options(
        dqlite_session_mode="deferred"
    )

    close_calls: list[object] = []
    original = eng.dialect.do_close

    def spy(dbapi_conn: object) -> None:
        close_calls.append(dbapi_conn)
        return original(dbapi_conn)  # type: ignore[arg-type]

    eng.dialect.do_close = spy  # type: ignore[assignment]

    conns = [eng.connect() for _ in range(3)]
    for c in conns:
        c.execute(text("SELECT 1")).scalar()
    for c in conns:
        c.close()  # return to pool, NOT close-the-connection

    eng.dispose()

    assert len(close_calls) == 3, (
        f"engine.dispose() should call do_close on every pooled slot; "
        f"got {len(close_calls)} calls (expected 3)."
    )


@pytest.mark.integration
async def test_async_engine_dispose_drains_pool(async_engine_url: str) -> None:
    """Async sibling of the dispose fan-out test."""
    eng = create_async_engine(async_engine_url, pool_size=2, max_overflow=0).execution_options(
        dqlite_session_mode="deferred"
    )

    close_calls: list[object] = []
    original = eng.sync_engine.dialect.do_close

    def spy(dbapi_conn: object) -> None:
        close_calls.append(dbapi_conn)
        return original(dbapi_conn)  # type: ignore[arg-type]

    eng.sync_engine.dialect.do_close = spy  # type: ignore[assignment]

    async with eng.connect() as a, eng.connect() as b:
        await a.execute(text("SELECT 1"))
        await b.execute(text("SELECT 1"))

    await eng.dispose()
    assert len(close_calls) == 2


@pytest.mark.integration
def test_before_cursor_execute_listener_fires(engine_url: str) -> None:
    """``before_cursor_execute`` must fire for each cursor execution;
    dialect plumbing that bypassed SA's cursor abstraction would
    silently break user listeners."""
    eng = create_engine(engine_url).execution_options(dqlite_session_mode="deferred")
    captured: list[str] = []

    @event.listens_for(eng, "before_cursor_execute")
    def _listener(
        conn: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        captured.append(statement)

    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 42"))
        assert any("SELECT 42" in s for s in captured), (
            f"before_cursor_execute did not capture the statement; saw {captured!r}"
        )
    finally:
        eng.dispose()


@pytest.mark.integration
def test_handle_error_listener_fires_on_disconnect(engine_url: str) -> None:
    """``handle_error`` must fire when SA classifies an exception as a disconnect."""
    eng = create_engine(engine_url, pool_pre_ping=False)
    seen_errors: list[object] = []

    @event.listens_for(eng, "handle_error")
    def _listener(ctx: object) -> None:
        seen_errors.append(ctx)

    try:
        with eng.connect() as conn:
            # Force-close the dbapi transport so SA's next round-trip disconnects.
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn is not None
            dbapi_conn.force_close_transport()
            with pytest.raises(DBAPIError):
                conn.execute(text("SELECT 1"))
        assert seen_errors, "handle_error listener did not fire on a forced disconnect"
    finally:
        eng.dispose()
