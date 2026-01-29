"""Integration tests for sync engine used inside an async context."""

import asyncio

import pytest
from sqlalchemy import create_engine, text


@pytest.mark.integration
class TestSyncEngineInAsyncContext:
    def test_sync_engine_inside_running_loop(self, engine_url: str) -> None:
        """Sync engine must work when called from inside a running event loop.

        This simulates the scenario where a sync SQLAlchemy engine is used
        during app startup inside an async server like uvicorn.
        """

        async def _run_inside_loop() -> str:
            engine = create_engine(engine_url)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 'ok'"))
                row = result.fetchone()
            engine.dispose()
            assert row is not None
            return str(row[0])

        result = asyncio.run(_run_inside_loop())
        assert result == "ok"
