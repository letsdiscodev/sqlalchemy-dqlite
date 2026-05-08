"""Pin: ``stream_results=True`` / ``yield_per`` get the documented
eager-buffer contract on the dqlite dialect.

The async ``AsyncAdaptedCursor`` adapter (``aio.py:42-72``) eagerly
fetches all rows during ``execute()`` within the greenlet context, then
serves fetch* calls synchronously from the buffer — a deliberate
divergence from SA's streaming contract that's a consequence of the
greenlet-eager-fetch pattern. The sync side has the same eager-fetch
shape via the underlying dbapi cursor.

ISSUE-DT5 dropped the peak-memory variant of this assertion (SA exposes
no per-dialect rejection hook; peak memory is hard to assert in a unit
test). This pin is narrower: the call returns ``len(rows) == N`` and
no rows are lost, which is the contract a future maintainer wiring up
true streaming on one side but forgetting the other could break.

Integration test against the cluster on localhost:9001 — exercises the
real adapter path on a real connection. No grey-box buffer inspection;
only the public-API row count.
"""

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

        # Even with stream_results=True, the dqlite adapter eagerly
        # buffers (documented divergence). The contract pin: no rows
        # lost, count matches.
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
    """``yield_per`` chunks rows from the already-buffered deque
    (``arraysize`` semantics on the adapter); the total row count
    must still match.
    """
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
