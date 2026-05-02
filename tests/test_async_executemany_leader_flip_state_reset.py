"""Pin: async ``executemany`` mid-loop leader-flip leaves cursor at
the no-result baseline.

The async adapter's ``executemany`` clears ``description`` /
``rowcount`` / ``lastrowid`` / ``_rows`` up-front. If a leader-flip
exception fires inside the underlying cursor's ``executemany``
mid-loop, the adapter's RETURNING-fetch branch never runs and the
cursor stays at the no-result baseline. Pin that contract so a future
refactor that "helpfully" preserves partial RETURNING rows on failure
cannot silently let a user think writes landed when they didn't.

The integration variant (real cluster, real leader demotion) requires
a ``cluster.demote_current_leader()`` helper that does not exist
today; this unit test mocks the underlying ``executemany`` and is the
load-bearing regression check.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterable, Sequence
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError as DbapiOperationalError
from dqlitewire.constants import (
    SQLITE_IOERR_LEADERSHIP_LOST,
    SQLITE_IOERR_NOT_LEADER,
)
from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _sync_await(coro: Any) -> Any:
    """Stand-in for SA's ``await_only`` so tests don't need a greenlet."""
    return asyncio.new_event_loop().run_until_complete(coro)


class _FakeAsyncCursor:
    """Minimal mock of :class:`dqlitedbapi.aio.AsyncCursor` shaped for the adapter."""

    def __init__(self, *, raise_on_executemany: BaseException | None = None) -> None:
        self.description: list[Any] | None = None
        self.rowcount = -1
        self.lastrowid: int | None = None
        self._raise = raise_on_executemany
        self.close = AsyncMock()
        self.fetchall = AsyncMock(return_value=[])

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> None:
        if self._raise is not None:
            raise self._raise


class _FakeAsyncConnection:
    def __init__(self, cursor: _FakeAsyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeAsyncCursor:
        return self._cursor


def _make_adapted_cursor(underlying: _FakeAsyncCursor) -> AsyncAdaptedCursor:
    fake_conn = _FakeAsyncConnection(underlying)
    adapted_conn = MagicMock()
    adapted_conn._connection = fake_conn

    # AsyncAdapted{Connection,Cursor}._handle_exception is the
    # adapter's central remap point and contractually re-raises (it
    # has a NoReturn return type). The cursor execute / executemany
    # methods route errors through it; without an explicit re-raise
    # in the mock, the cursor would silently swallow the leader-flip
    # error this test is asserting on.
    def _reraise(error: BaseException) -> None:
        raise error

    adapted_conn._handle_exception = _reraise
    return AsyncAdaptedCursor(adapted_conn)


@pytest.mark.parametrize("code", [SQLITE_IOERR_NOT_LEADER, SQLITE_IOERR_LEADERSHIP_LOST])
class TestAsyncExecutemanyLeaderFlipResetState:
    def test_state_reset_on_leader_flip(self, monkeypatch: pytest.MonkeyPatch, code: int) -> None:
        from sqlalchemydqlite import aio as aio_mod

        monkeypatch.setattr(aio_mod, "await_only", _sync_await)

        underlying = _FakeAsyncCursor(
            raise_on_executemany=DbapiOperationalError("leadership lost", code=code)
        )
        cur = _make_adapted_cursor(underlying)

        # Pre-populate state to ensure the up-front reset takes effect.
        cur.description = [("col", None, None, None, None, None, None)]
        cur.rowcount = 99
        cur.lastrowid = 42
        cur._rows.extend([(1,), (2,)])

        with pytest.raises(DbapiOperationalError) as ei:
            cur.executemany("INSERT INTO t (v) VALUES (:v) RETURNING id", [{"v": "a"}])

        assert ei.value.code == code
        # Pin: the cursor stays at the no-result baseline. The up-front
        # reset has already cleared everything; the leader-flip exception
        # propagates without populating description/rows.
        assert cur.description is None
        assert cur.rowcount == -1
        # ``lastrowid`` is sticky across non-INSERT executes — including
        # failed ones — to match stdlib ``sqlite3.Cursor.lastrowid``
        # and the dbapi-layer cursor's contract. The pre-populated
        # value (42) survives a failing executemany.
        assert cur.lastrowid == 42
        assert cur._rows == deque()
        # Cursor was closed in the finally block.
        underlying.close.assert_awaited_once()
