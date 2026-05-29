"""SA ``commit`` / ``rollback`` event listeners fire on dialect transactions.

A future override of do_commit/do_rollback that fails to re-emit events would
silently break user hooks (cache invalidation, audit, telemetry).
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

            # Async event registration goes through the sync_engine's event system.
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
