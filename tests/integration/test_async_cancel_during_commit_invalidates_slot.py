"""End-to-end pin: a CancelledError fired during the COMMIT
round-trip causes SA's pool to fire its ``invalidate`` event for
the slot, mark the SA Connection ``invalidated``, and let
``engine.dispose`` complete cleanly without hanging.

Sibling test ``test_async_cancel_inside_engine_begin.py`` covers
cancellation in the BODY (the ROLLBACK path on ``__aexit__``);
this test covers the orthogonal path where the cancellation lands
during the implicit COMMIT.

The contract under test:

1. ``CancelledError`` propagates through
   ``AsyncAdaptedConnection.commit`` unchanged.
2. SA classifies ``CancelledError`` as ``is_exit_exception`` →
   ``Connection._is_disconnect = True`` → ``self.invalidate(e)``
   in the ``finally`` clause of ``_handle_dbapi_exception``.
3. The pool slot is discarded — the pool's ``invalidate`` event
   fires exactly once.
4. The SA ``Connection`` instance has ``invalidated == True``.
5. ``engine.dispose`` post-cancel completes without hanging.
6. The next acquire returns a fresh, healthy connection.

Methodology: rather than tuning a real cluster's COMMIT round-trip
to land inside an ``asyncio.timeout``, we monkey-patch the dbapi
``AsyncConnection.commit`` to add a 2-second ``asyncio.sleep`` BEFORE
the real commit begins. The outer 0.1s ``asyncio.timeout`` then
fires deterministically inside the sleep — same surface the
adapter sees as "cancellation while awaiting commit".
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
        """Pin: ``event.listen(pool, "invalidate", ...)`` fires
        exactly once with the cancellation as the exception arg."""
        from dqlitedbapi.aio.connection import AsyncConnection

        original_commit = AsyncConnection.commit

        async def _slow_commit(self: AsyncConnection) -> None:
            # Park BEFORE the real commit starts; the outer timeout
            # lands inside this sleep.
            await asyncio.sleep(2.0)
            return await original_commit(self)

        engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
        try:
            # Set up the table BEFORE patching commit (otherwise the
            # CREATE TABLE block's commit would also park 2s).
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS commit_cancel"))
                await conn.execute(text("CREATE TABLE commit_cancel (id INTEGER PRIMARY KEY)"))

            # Capture invalidate events on the underlying sync_engine's
            # pool — that's where SA fires the public event.
            invalidate_calls: list[tuple[Any, Any]] = []

            @event.listens_for(engine.sync_engine.pool, "invalidate")
            def _on_invalidate(dbapi_conn: Any, conn_record: Any, exception: Any) -> None:
                invalidate_calls.append((conn_record, exception))

            monkeypatch.setattr(AsyncConnection, "commit", _slow_commit)

            with pytest.raises(asyncio.TimeoutError):
                async with asyncio.timeout(0.1):
                    async with engine.begin() as conn:
                        await conn.execute(text("INSERT INTO commit_cancel VALUES (1)"))
                        # Implicit COMMIT runs on __aexit__; the
                        # patched commit parks 2s; the 0.1s timeout
                        # fires inside the sleep.

            # The pool MUST have invalidated the slot exactly once.
            assert len(invalidate_calls) == 1, (
                f"expected exactly one invalidate event, got "
                f"{len(invalidate_calls)}: {invalidate_calls!r}"
            )
            exc = invalidate_calls[0][1]
            assert isinstance(exc, (asyncio.CancelledError, TimeoutError)), (
                f"invalidate fired with unexpected exception type: {type(exc).__name__}"
            )

            # Restore commit so the post-cancel acquire works normally.
            monkeypatch.setattr(AsyncConnection, "commit", original_commit)

            # Post-cancel acquire returns a healthy slot.
            async with engine.begin() as conn:
                rows = (await conn.execute(text("SELECT id FROM commit_cancel"))).all()
            # Don't assert the row count — tx-065's load-bearing point
            # is "server-side outcome is ambiguous; the SLOT is
            # invalidated regardless." The SELECT must work cleanly.
            assert isinstance(rows, list)
        finally:
            await engine.dispose()

    async def test_engine_connect_invalidated_flag_after_commit_cancel(
        self, async_engine_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pin: SA's ``AsyncConnection.invalidated`` is True on the
        same connection instance after a commit-cancel. Distinct
        from the pool-event pin: this asserts the SA-layer Connection
        view of the slot, not just the pool's bookkeeping."""
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

            # Use engine.connect() + explicit conn.begin() so the
            # connection object is reachable post-cancel for the
            # ``invalidated`` assertion.
            async with engine.connect() as conn:
                tx = await conn.begin()
                await conn.execute(text("INSERT INTO commit_cancel_inv VALUES (1)"))
                with pytest.raises(asyncio.TimeoutError):
                    async with asyncio.timeout(0.1):
                        await tx.commit()

                assert conn.invalidated is True
        finally:
            # Restore commit before dispose so the dispose path is
            # not held up by the slow_commit patch.
            monkeypatch.setattr(AsyncConnection, "commit", original_commit)
            await engine.dispose()

    async def test_engine_dispose_completes_after_commit_cancel(
        self, async_engine_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Smoke: after a commit-cancel, ``engine.dispose`` completes
        within a small bound — i.e. ``has_terminate=True``'s
        ``do_terminate`` path is reachable on a commit-cancelled slot
        and does not deadlock or hang waiting for the cancelled
        commit's response."""
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

            # Restore commit; dispose should not wait on it but be
            # paranoid in case any other path tries to use it.
            monkeypatch.setattr(AsyncConnection, "commit", original_commit)

            # Dispose MUST complete promptly — bound at 1s, well below
            # the 2s slow-commit sleep that we'd see if dispose
            # somehow awaited the cancelled commit.
            async with asyncio.timeout(1.0):
                await engine.dispose()
        except BaseException:
            monkeypatch.setattr(AsyncConnection, "commit", original_commit)
            raise
