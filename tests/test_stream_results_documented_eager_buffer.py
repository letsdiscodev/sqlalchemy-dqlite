"""``stream_results=True`` / ``yield_per`` still eagerly buffer all rows on
the dqlite dialect (a deliberate divergence from SA streaming); the pin is
that no rows are lost (count matches), checked via the public-API row count."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

_ROW_COUNT = 100


@pytest.mark.integration
def test_sync_stream_results_returns_all_rows(cluster_address: str) -> None:
    host, port = cluster_address.split(":")
    engine = create_engine(f"dqlite://{host}:{port}/default")
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS stream_results_pin"))
            conn.execute(text("CREATE TABLE stream_results_pin (id INTEGER PRIMARY KEY)"))
            for i in range(_ROW_COUNT):
                conn.execute(
                    text("INSERT INTO stream_results_pin (id) VALUES (:i)"),
                    {"i": i},
                )

        with engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(
                text("SELECT id FROM stream_results_pin ORDER BY id")
            )
            rows = list(result)

        assert len(rows) == _ROW_COUNT
        assert [r[0] for r in rows] == list(range(_ROW_COUNT))
    finally:
        engine.dispose()


@pytest.mark.integration
async def test_async_stream_results_returns_all_rows(cluster_address: str) -> None:
    host, port = cluster_address.split(":")
    engine = create_async_engine(f"dqlite+aio://{host}:{port}/default")
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS stream_results_pin_async"))
            await conn.execute(
                text("CREATE TABLE stream_results_pin_async (id INTEGER PRIMARY KEY)")
            )
            for i in range(_ROW_COUNT):
                await conn.execute(
                    text("INSERT INTO stream_results_pin_async (id) VALUES (:i)"),
                    {"i": i},
                )

        async with engine.connect() as conn:
            streaming_conn = await conn.execution_options(stream_results=True)
            result = await streaming_conn.execute(
                text("SELECT id FROM stream_results_pin_async ORDER BY id")
            )
            rows = list(result)

        assert len(rows) == _ROW_COUNT
        assert [r[0] for r in rows] == list(range(_ROW_COUNT))
    finally:
        await engine.dispose()


@pytest.mark.integration
def test_sync_yield_per_returns_all_rows(cluster_address: str) -> None:
    """``yield_per`` chunks from the already-buffered deque; count still matches."""
    host, port = cluster_address.split(":")
    engine = create_engine(f"dqlite://{host}:{port}/default")
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS yield_per_pin"))
            conn.execute(text("CREATE TABLE yield_per_pin (id INTEGER PRIMARY KEY)"))
            for i in range(_ROW_COUNT):
                conn.execute(
                    text("INSERT INTO yield_per_pin (id) VALUES (:i)"),
                    {"i": i},
                )

        with engine.connect() as conn:
            result = conn.execution_options(yield_per=10).execute(
                text("SELECT id FROM yield_per_pin ORDER BY id")
            )
            rows = list(result)

        assert len(rows) == _ROW_COUNT
        assert [r[0] for r in rows] == list(range(_ROW_COUNT))
    finally:
        engine.dispose()
