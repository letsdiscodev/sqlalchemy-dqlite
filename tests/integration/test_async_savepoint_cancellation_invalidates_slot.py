"""End-to-end pin: a CancelledError fired during a SAVEPOINT-family
operation invalidates the SA pool slot.

Sibling tests cover BEGIN cancellation
(``test_async_cancel_inside_engine_begin.py``) and COMMIT
cancellation (``test_async_cancel_during_commit_invalidates_slot.py``).
This file covers the three SAVEPOINT-related round-trips that
SQLAlchemy's ``Connection.begin_nested()`` triggers via the inherited
``DefaultDialect`` hooks:

- ``do_savepoint`` emits ``SAVEPOINT <name>``
- ``do_release_savepoint`` emits ``RELEASE SAVEPOINT <name>``
- ``do_rollback_to_savepoint`` emits ``ROLLBACK TO SAVEPOINT <name>``

The contract under test for each window:

1. ``CancelledError`` / ``TimeoutError`` propagates through SA's
   nested-tx context manager unchanged.
2. SA classifies the cancel as ``is_exit_exception`` and invalidates
   the underlying connection slot.
3. ``engine.dispose`` post-cancel completes without hanging.
4. The next acquire returns a fresh, healthy connection.

Methodology mirrors the COMMIT-cancel sibling: monkey-patch the
dbapi cursor's ``execute`` to inject a ``sleep`` when the SQL
prefix matches the SAVEPOINT family, then drive the SA path under
an outer ``asyncio.timeout`` short enough to land inside the sleep.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def _slowing_execute(prefix: str, original: Any, sleep_seconds: float = 2.0) -> Any:
    """Build a replacement for ``AsyncCursor.execute`` that sleeps when
    the SQL leading-prefix matches.

    Other statements (CREATE TABLE, INSERT, SELECT, BEGIN) pass
    through unchanged — a blanket sleep would also park CREATE
    TABLE etc. and stop the test from progressing to the SAVEPOINT
    statement.
    """

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
            # Setup table BEFORE patching execute so the CREATE TABLE
            # is not parked.
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
                            # SAVEPOINT round-trip happens above. If the
                            # patched prefix is "SAVEPOINT", the
                            # sleep parks here and the timeout fires
                            # inside it.
                            if trigger_release:
                                await conn.execute(text("INSERT INTO sp_cancel VALUES (2)"))
                                # RELEASE SAVEPOINT round-trip parks here.
                                await sp.commit()
                            elif trigger_rollback:
                                await conn.execute(text("INSERT INTO sp_cancel VALUES (2)"))
                                # ROLLBACK TO SAVEPOINT round-trip parks here.
                                await sp.rollback()

            # Restore execute so the post-cancel acquire works normally.
            monkeypatch.setattr(AsyncCursor, "execute", original_execute)

            # Post-cancel acquire returns a healthy connection.
            async with engine.connect() as conn:
                row = (await conn.execute(text("SELECT 1"))).scalar()
                assert row == 1
        finally:
            await engine.dispose()
