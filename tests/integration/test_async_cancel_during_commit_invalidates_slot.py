"""A CancelledError during the COMMIT round-trip invalidates the SA pool slot.

Sibling ``test_async_cancel_inside_engine_begin.py`` covers cancellation in the
body (ROLLBACK path); this covers cancellation during the implicit COMMIT.

The slow_commit monkey-patch sleeps 2s before the real commit so the outer 0.1s
``asyncio.timeout`` fires deterministically inside the sleep.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
class TestAsyncCancelDuringCommitInvalidatesSlot:
    async def test_invalidate_event_fires_on_commit_cancel(
        self, async_engine_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The pool ``invalidate`` event fires exactly once, with the
        cancellation as the exception arg."""
        from dqlitedbapi.aio.connection import AsyncConnection

        original_commit = AsyncConnection.commit

        async def _slow_commit(self: AsyncConnection) -> None:
            await asyncio.sleep(2.0)
            return await original_commit(self)

        engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
        try:
            # Set up the table before patching commit, else its commit parks 2s.
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS commit_cancel"))
                await conn.execute(text("CREATE TABLE commit_cancel (id INTEGER PRIMARY KEY)"))

            # SA fires the public invalidate event on the sync_engine's pool.
            invalidate_calls: list[tuple[Any, Any]] = []

            @event.listens_for(engine.sync_engine.pool, "invalidate")
            def _on_invalidate(dbapi_conn: Any, conn_record: Any, exception: Any) -> None:
                invalidate_calls.append((conn_record, exception))

            monkeypatch.setattr(AsyncConnection, "commit", _slow_commit)

            with pytest.raises(asyncio.TimeoutError):
                async with asyncio.timeout(0.1):
                    async with engine.begin() as conn:
                        await conn.execute(text("INSERT INTO commit_cancel VALUES (1)"))
                        # Implicit COMMIT on __aexit__ parks 2s; timeout fires.

            assert len(invalidate_calls) == 1, (
                f"expected exactly one invalidate event, got "
                f"{len(invalidate_calls)}: {invalidate_calls!r}"
            )
            exc = invalidate_calls[0][1]
            assert isinstance(exc, (asyncio.CancelledError, TimeoutError)), (
                f"invalidate fired with unexpected exception type: {type(exc).__name__}"
            )

            monkeypatch.setattr(AsyncConnection, "commit", original_commit)

            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM commit_cancel"))).all()
            # Don't assert the row count: the server-side commit outcome is
            # ambiguous; the point is the slot is invalidated regardless.
            assert isinstance(rows, list)
        finally:
            await engine.dispose()

    async def test_engine_connect_invalidated_flag_after_commit_cancel(
        self, async_engine_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SA ``Connection.invalidated`` is True after a commit-cancel
        (the SA-layer view of the slot, not just the pool's bookkeeping)."""
        from dqlitedbapi.aio.connection import AsyncConnection

        original_commit = AsyncConnection.commit

        async def _slow_commit(self: AsyncConnection) -> None:
            await asyncio.sleep(2.0)
            return await original_commit(self)

        engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS commit_cancel_inv"))
                await conn.execute(text("CREATE TABLE commit_cancel_inv (id INTEGER PRIMARY KEY)"))

            monkeypatch.setattr(AsyncConnection, "commit", _slow_commit)

            # engine.connect() + explicit begin keeps the conn object reachable
            # post-cancel for the ``invalidated`` assertion.
            async with engine.connect() as conn:
                tx = await conn.begin()
                await conn.execute(text("INSERT INTO commit_cancel_inv VALUES (1)"))
                with pytest.raises(asyncio.TimeoutError):
                    async with asyncio.timeout(0.1):
                        await tx.commit()

                assert conn.invalidated is True
        finally:
            # Restore commit before dispose so it isn't held up by the patch.
            monkeypatch.setattr(AsyncConnection, "commit", original_commit)
            await engine.dispose()

    async def test_engine_dispose_completes_after_commit_cancel(
        self, async_engine_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After a commit-cancel, ``engine.dispose`` completes within a small
        bound — the do_terminate path doesn't hang on the cancelled commit."""
        from dqlitedbapi.aio.connection import AsyncConnection

        original_commit = AsyncConnection.commit

        async def _slow_commit(self: AsyncConnection) -> None:
            await asyncio.sleep(2.0)
            return await original_commit(self)

        engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            monkeypatch.setattr(AsyncConnection, "commit", _slow_commit)

            with pytest.raises(asyncio.TimeoutError):
                async with asyncio.timeout(0.1):
                    async with engine.begin() as conn:
                        await conn.execute(text("SELECT 1"))

            monkeypatch.setattr(AsyncConnection, "commit", original_commit)

            # Bound dispose at 1s, well below the 2s slow-commit sleep we'd
            # see if dispose somehow awaited the cancelled commit.
            async with asyncio.timeout(1.0):
                await engine.dispose()
        except BaseException:
            monkeypatch.setattr(AsyncConnection, "commit", original_commit)
            raise
