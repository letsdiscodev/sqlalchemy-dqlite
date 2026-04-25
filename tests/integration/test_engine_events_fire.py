"""SA event listeners (``commit``, ``rollback``) fire on dialect
transactions.

The dialect inherits ``do_commit`` / ``do_rollback`` from
``DefaultDialect``; SA's wrapper around those emits the public events
via the engine's event system. The contract is "if you register
``@event.listens_for(engine, 'commit')`` the listener fires on a
successful commit."

A future refactor that overrides ``do_commit`` / ``do_rollback``
without re-emitting events would silently break user hooks (cache
invalidation, audit logs, telemetry). Pin the contract for both
sync and async engines so that regression is loud.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
class TestEngineCommitRollbackEventsFire:
    def test_sync_commit_event_fires(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        try:
            fired: list[str] = []

            @event.listens_for(engine, "commit")
            def _on_commit(conn) -> None:  # noqa: ARG001
                fired.append("commit")

            with engine.begin() as conn:
                conn.execute(text("SELECT 1"))
            # engine.begin() commits cleanly on exit.
            assert fired == ["commit"]
        finally:
            engine.dispose()

    def test_sync_rollback_event_fires_on_exception(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        try:
            fired: list[str] = []

            @event.listens_for(engine, "rollback")
            def _on_rollback(conn) -> None:  # noqa: ARG001
                fired.append("rollback")

            with pytest.raises(RuntimeError, match="user error"), engine.begin() as conn:
                conn.execute(text("SELECT 1"))
                raise RuntimeError("user error")
            assert fired == ["rollback"]
        finally:
            engine.dispose()

    async def test_async_commit_event_fires(self, async_engine_url: str) -> None:
        engine = create_async_engine(async_engine_url)
        try:
            fired: list[str] = []

            # Register on the underlying sync_engine — SA's async
            # event registration goes through the sync engine's
            # event system.
            @event.listens_for(engine.sync_engine, "commit")
            def _on_commit(conn) -> None:  # noqa: ARG001
                fired.append("commit")

            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            assert fired == ["commit"]
        finally:
            await engine.dispose()

    async def test_async_rollback_event_fires_on_exception(self, async_engine_url: str) -> None:
        engine = create_async_engine(async_engine_url)
        try:
            fired: list[str] = []

            @event.listens_for(engine.sync_engine, "rollback")
            def _on_rollback(conn) -> None:  # noqa: ARG001
                fired.append("rollback")

            with pytest.raises(RuntimeError, match="user error"):
                async with engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))
                    raise RuntimeError("user error")
            assert fired == ["rollback"]
        finally:
            await engine.dispose()
