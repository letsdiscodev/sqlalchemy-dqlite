"""Integration tests for ``AsyncAdaptedCursor.executemany`` against a real server.

RETURNING is tested at the dbapi layer only: SA 2.0+ insertmanyvalues rewrites
``INSERT ... RETURNING`` into a multi-row INSERT that dqlite's gateway rejects
with "nonempty statement tail" in some orderings.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
class TestAsyncExecutemany:
    async def test_async_executemany_rowcount_sum(self, async_engine_url: str) -> None:
        """Plain (non-returning) executemany sums rowcount across iterations."""
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_em_rc"))
                await conn.execute(
                    text("CREATE TABLE async_em_rc (id INTEGER PRIMARY KEY, val TEXT)")
                )
                result = await conn.execute(
                    text("INSERT INTO async_em_rc (val) VALUES (:v)"),
                    [{"v": f"r{i}"} for i in range(5)],
                )
                assert result.rowcount == 5

                count_result = await conn.execute(text("SELECT COUNT(*) FROM async_em_rc"))
                assert count_result.scalar() == 5
        finally:
            await engine.dispose()

    async def test_async_executemany_sequential_tasks_no_state_leak(
        self, async_engine_url: str
    ) -> None:
        """Sequential executemany from two tasks must not leak cursor state.

        Sequential (not concurrent) because dqlite serializes writes via Raft;
        concurrent writers would hit ``database is locked``, unrelated here."""
        engine = create_async_engine(async_engine_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS async_em_sequential"))
                await conn.execute(
                    text(
                        "CREATE TABLE async_em_sequential "
                        "(id INTEGER PRIMARY KEY, owner TEXT, val INTEGER)"
                    )
                )

            async def worker(owner: str, n: int) -> int:
                async with engine.begin() as conn:
                    result = await conn.execute(
                        text("INSERT INTO async_em_sequential (owner, val) VALUES (:o, :v)"),
                        [{"o": owner, "v": i} for i in range(n)],
                    )
                    return int(result.rowcount)

            a_rc = await worker("a", 3)
            b_rc = await worker("b", 4)

            assert a_rc == 3
            assert b_rc == 4

            async with engine.begin() as conn:
                result = await conn.execute(
                    text("SELECT owner, COUNT(*) FROM async_em_sequential GROUP BY owner")
                )
                counts = dict(result.fetchall())  # type: ignore[arg-type,var-annotated]
                assert counts == {"a": 3, "b": 4}, (
                    f"executemany leaked rows between sequential tasks: "
                    f"expected {{'a': 3, 'b': 4}}, got {counts}"
                )
        finally:
            await engine.dispose()
