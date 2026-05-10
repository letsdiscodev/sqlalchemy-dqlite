"""Integration coverage for SA pool features that interact with the
dialect's ``do_close`` / ``do_terminate`` / ``do_ping`` hooks.

Existing coverage: ``test_pool_pre_ping_leader_flip.py`` exercises
pre-ping; ``test_pool_reset_on_return.py`` covers checkin reset;
``test_async_cancel_during_commit_invalidates_slot.py`` covers
invalidation. This file pins the still-uncovered scenarios:

- ``pool_recycle``: recycled slots must invoke ``do_terminate`` (not
  ``do_close``) per SA's ``__close(terminate=True)`` path.
- ``engine.dispose()``: every queued slot must invoke ``do_close``
  (one per pooled connection).
- ``before_cursor_execute`` and ``handle_error`` SA events fire
  through the dialect.
"""

from __future__ import annotations

import time

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
def test_sync_pool_recycle_calls_do_terminate(engine_url: str) -> None:
    """SA's recycle path goes through ``__close(terminate=True)`` →
    ``dialect.do_terminate(conn)``. A regression that flipped this
    to ``terminate=False`` (using ``do_close``'s graceful close
    path) would silently extend recycle latency. Instrument
    ``do_terminate`` and verify it fires when a slot ages past
    ``pool_recycle``."""
    eng = create_engine(engine_url, pool_recycle=0.1, pool_size=1, max_overflow=0)
    try:
        terminate_calls: list[object] = []
        original = eng.dialect.do_terminate

        def spy(dbapi_conn: object) -> None:
            terminate_calls.append(dbapi_conn)
            return original(dbapi_conn)  # type: ignore[arg-type]

        eng.dialect.do_terminate = spy  # type: ignore[assignment]

        # Warm one slot.
        with eng.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        # Sleep past recycle window.
        time.sleep(0.2)
        # Next checkout triggers recycle of the stale slot.
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
    """SA's ``QueuePool.dispose()`` drains every queued slot and
    invokes ``conn.close()``, routing through ``dialect.do_close``
    (NOT ``do_terminate``, since dispose passes ``terminate=False``).
    Warm multiple slots, dispose, count do_close calls."""
    eng = create_engine(engine_url, pool_size=3, max_overflow=0)

    close_calls: list[object] = []
    original = eng.dialect.do_close

    def spy(dbapi_conn: object) -> None:
        close_calls.append(dbapi_conn)
        return original(dbapi_conn)  # type: ignore[arg-type]

    eng.dialect.do_close = spy  # type: ignore[assignment]

    # Warm 3 slots concurrently.
    conns = [eng.connect() for _ in range(3)]
    for c in conns:
        c.execute(text("SELECT 1")).scalar()
    for c in conns:
        c.close()  # return to pool, NOT close-the-connection

    # Dispose now drains the queue.
    eng.dispose()

    assert len(close_calls) == 3, (
        f"engine.dispose() should call do_close on every pooled slot; "
        f"got {len(close_calls)} calls (expected 3)."
    )


@pytest.mark.integration
async def test_async_engine_dispose_drains_pool(async_engine_url: str) -> None:
    """Async sibling of the dispose fan-out test."""
    eng = create_async_engine(async_engine_url, pool_size=2, max_overflow=0)

    close_calls: list[object] = []
    # AsyncAdaptedConnection.close() routes through the dbapi
    # AsyncConnection.close(); instrument the dialect's do_close
    # which is the documented adapter point.
    original = eng.sync_engine.dialect.do_close

    def spy(dbapi_conn: object) -> None:
        close_calls.append(dbapi_conn)
        return original(dbapi_conn)  # type: ignore[arg-type]

    eng.sync_engine.dialect.do_close = spy  # type: ignore[assignment]

    # Warm 2 slots concurrently.
    async with eng.connect() as a, eng.connect() as b:
        await a.execute(text("SELECT 1"))
        await b.execute(text("SELECT 1"))

    await eng.dispose()
    assert len(close_calls) == 2


@pytest.mark.integration
def test_before_cursor_execute_listener_fires(engine_url: str) -> None:
    """The ``before_cursor_execute`` event must fire for each cursor
    execution. A regression in dialect plumbing that bypassed the
    SA cursor abstraction would silently break user listeners."""
    eng = create_engine(engine_url)
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
    """The ``handle_error`` event must fire when SA classifies an
    exception as a disconnect. Force a disconnect by closing the
    underlying transport out from under SA, then attempt a query."""
    eng = create_engine(engine_url, pool_pre_ping=False)
    seen_errors: list[object] = []

    @event.listens_for(eng, "handle_error")
    def _listener(ctx: object) -> None:
        seen_errors.append(ctx)

    try:
        with eng.connect() as conn:
            # Reach in and force-close the dbapi transport so SA's
            # next round-trip surfaces a disconnect.
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn is not None
            dbapi_conn.force_close_transport()
            with pytest.raises(DBAPIError):
                conn.execute(text("SELECT 1"))
        assert seen_errors, "handle_error listener did not fire on a forced disconnect"
    finally:
        eng.dispose()
