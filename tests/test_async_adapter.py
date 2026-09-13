"""The async adapter over a scripted stand-in for ``dqlitedbapi.aio.AsyncConnection``."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy.exc import ArgumentError
from sqlalchemy.util.concurrency import greenlet_spawn

import dqlitedbapi.exceptions as dbapi_exc
from dqlitedbapi import aio as dbapi_aio
from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor, DqliteDialect_aio


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.description: Any = None
        self.rowcount = -1
        self.lastrowid: int | None = None
        self.arraysize = 1
        self._rows: list[tuple[Any, ...]] = []
        self.closed = False

    async def execute(self, sql: str, params: Any = None) -> FakeCursor:
        self.connection.log.append(("execute", sql, params))
        if self.connection.fail is not None:
            raise self.connection.fail
        if sql.startswith("SELECT"):
            self.description = (("x", 4, None, None, None, None, None),)
            self._rows = [(1,), (2,), (3,)]
            self.rowcount = -1
        else:
            self.description = None
            self.rowcount = 1
            self.lastrowid = 7
        return self

    async def executemany(self, sql: str, seq: Any) -> FakeCursor:
        seq = list(seq)
        self.connection.log.append(("executemany", sql, seq))
        if "RETURNING" in sql:
            self.description = (("id", 4, None, None, None, None, None),)
            self._rows = [(i,) for i, _ in enumerate(seq)]
        self.rowcount = len(seq)
        return self

    async def fetchall(self) -> list[tuple[Any, ...]]:
        rows, self._rows = self._rows, []
        return rows

    def close(self) -> None:
        self.closed = True

    def nextset(self) -> None:
        raise dbapi_exc.NotSupportedError("nextset")


class FakeConnection:
    def __init__(self) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.fail: BaseException | None = None
        self.closed = False
        self.forced = False
        self.connected = False

    async def connect(self) -> None:
        if self.fail is not None:
            raise self.fail
        self.connected = True

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    async def commit(self) -> None:
        self.log.append(("commit",))

    async def rollback(self) -> None:
        self.log.append(("rollback",))

    async def close(self) -> None:
        if self.fail is not None:
            raise self.fail
        self.closed = True

    def force_close_transport(self) -> None:
        self.forced = True
        self.closed = True


def adapted() -> tuple[AsyncAdaptedConnection, FakeConnection]:
    inner = FakeConnection()
    return AsyncAdaptedConnection(dbapi_aio, inner), inner  # type: ignore[arg-type]


async def test_cursor_buffers_rows_and_reports_dml_results() -> None:
    conn, inner = adapted()

    def use() -> None:
        cur = conn.cursor()
        cur.execute("SELECT x FROM t")
        assert cur.description is not None and cur.description[0][0] == "x"
        assert cur.fetchone() == (1,)
        assert cur.fetchmany(5) == [(2,), (3,)]
        assert cur.fetchall() == []
        cur.execute("INSERT INTO t VALUES (?)", (1,))
        assert (cur.rowcount, cur.lastrowid, cur.description) == (1, 7, None)
        cur.executemany("INSERT INTO t VALUES (?) RETURNING id", [(1,), (2,)])
        assert cur.fetchall() == [(0,), (1,)]
        cur.setinputsizes(None)
        cur.close()

    await greenlet_spawn(use)
    assert [entry[0] for entry in inner.log] == ["execute", "execute", "executemany"]


async def test_connection_delegates_and_rejects_server_side_cursors() -> None:
    conn, inner = adapted()

    def use() -> None:
        assert conn.isolation_level == "SERIALIZABLE" and conn.autocommit is False
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            conn.autocommit = True
        with pytest.raises(dbapi_exc.NotSupportedError, match="Server-side"):
            conn.cursor(server_side=True)
        assert isinstance(conn.cursor(), AsyncAdaptedCursor)
        conn.commit()
        conn.rollback()
        conn.close()

    await greenlet_spawn(use)
    assert inner.log == [("commit",), ("rollback",)] and inner.closed


async def test_dbapi_errors_propagate_unchanged() -> None:
    conn, inner = adapted()
    inner.fail = dbapi_exc.OperationalError("boom", code=1)

    def use() -> None:
        with pytest.raises(dbapi_exc.OperationalError, match="boom") as info:
            conn.cursor().execute("SELECT 1")
        assert info.value.code == 1

    await greenlet_spawn(use)


async def test_terminate_forces_the_transport_when_close_fails() -> None:
    conn, inner = adapted()
    inner.fail = dbapi_exc.OperationalError("lost")
    await greenlet_spawn(conn.terminate)
    assert inner.forced
    conn2, inner2 = adapted()
    conn2.terminate()  # outside a greenlet: the garbage-collection path
    assert inner2.forced


async def test_dialect_connect_awaits_the_wire_and_cleans_up_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    d = DqliteDialect_aio()
    d.dbapi = dbapi_aio  # type: ignore[assignment]
    made: list[FakeConnection] = []

    def fake_connect(**kwargs: Any) -> FakeConnection:
        made.append(FakeConnection())
        return made[-1]

    monkeypatch.setattr(dbapi_aio, "connect", fake_connect)
    wrapped = await greenlet_spawn(d.connect, address="h:1", database="db")
    assert isinstance(wrapped, AsyncAdaptedConnection) and made[0].connected
    assert d.get_driver_connection(wrapped) is made[0]

    def failing_creator(**kwargs: Any) -> FakeConnection:
        conn = FakeConnection()
        conn.fail = OSError("refused")
        made.append(conn)
        return conn

    with pytest.raises(OSError):
        await greenlet_spawn(d.connect, address="h:1", async_creator_fn=failing_creator)
    assert made[-1].forced

    async def async_creator(**kwargs: Any) -> FakeConnection:
        return FakeConnection()

    with pytest.raises(ArgumentError, match="async_creator_fn"):
        await greenlet_spawn(d.connect, address="h:1", async_creator_fn=async_creator)


async def test_cancellation_reaches_the_caller() -> None:
    conn, inner = adapted()
    inner.fail = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await greenlet_spawn(lambda: conn.cursor().execute("SELECT 1"))
