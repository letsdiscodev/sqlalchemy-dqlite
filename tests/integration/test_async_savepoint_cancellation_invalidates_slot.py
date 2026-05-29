"""A CancelledError during a SAVEPOINT-family op invalidates the SA pool slot.

Covers the three round-trips ``begin_nested()`` triggers (SAVEPOINT,
RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT); BEGIN and COMMIT cancellation are
covered in sibling files. The cursor ``execute`` patch sleeps when the SQL
prefix matches so the outer ``asyncio.timeout`` lands inside the sleep.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def _slowing_execute(prefix: str, original: Any, sleep_seconds: float = 2.0) -> Any:
    """Replacement for ``AsyncCursor.execute`` that sleeps only when the SQL
    leading-prefix matches, so setup statements still run."""

    async def _patched(self: Any, operation: str, parameters: Sequence[Any] | None = None) -> Any:
        if operation.lstrip().upper().startswith(prefix.upper()):
            await asyncio.sleep(sleep_seconds)
        return await original(self, operation, parameters)

    return _patched


@pytest.mark.integration
class TestAsyncSavepointCancellationInvalidatesSlot:
    @pytest.mark.parametrize(
        ("prefix", "trigger_savepoint", "trigger_release", "trigger_rollback"),
        [
            ("SAVEPOINT", True, False, False),
            ("RELEASE SAVEPOINT", False, True, False),
            ("ROLLBACK TO SAVEPOINT", False, False, True),
        ],
    )
    async def test_cancel_during_savepoint_family_invalidates_slot(
        self,
        async_engine_url: str,
        monkeypatch: pytest.MonkeyPatch,
        prefix: str,
        trigger_savepoint: bool,
        trigger_release: bool,
        trigger_rollback: bool,
    ) -> None:
        from dqlitedbapi.aio.cursor import AsyncCursor

        original_execute = AsyncCursor.execute

        engine = create_async_engine(async_engine_url, pool_size=1, max_overflow=0)
        try:
            # Set up the table before patching execute so it isn't parked.
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS sp_cancel"))
                await conn.execute(text("CREATE TABLE sp_cancel (id INTEGER PRIMARY KEY)"))

            monkeypatch.setattr(AsyncCursor, "execute", _slowing_execute(prefix, original_execute))

            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.1):
                    async with engine.connect() as conn:
                        async with conn.begin():
                            await conn.execute(text("INSERT INTO sp_cancel VALUES (1)"))
                            sp = await conn.begin_nested()
                            # SAVEPOINT round-trip ran above; if prefix is
                            # "SAVEPOINT" the timeout already fired in its sleep.
                            if trigger_release:
                                await conn.execute(text("INSERT INTO sp_cancel VALUES (2)"))
                                await sp.commit()
                            elif trigger_rollback:
                                await conn.execute(text("INSERT INTO sp_cancel VALUES (2)"))
                                await sp.rollback()

            monkeypatch.setattr(AsyncCursor, "execute", original_execute)

            async with engine.connect() as conn:
                row = (await conn.execute(text("SELECT 1"))).scalar()
                assert row == 1
        finally:
            await engine.dispose()
