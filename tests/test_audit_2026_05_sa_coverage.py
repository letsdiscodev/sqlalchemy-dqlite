"""Coverage gaps surfaced by the 2026-05 SA-side audit pass."""

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.engine.url import make_url

from dqlitedbapi.exceptions import DatabaseError
from sqlalchemydqlite.aio import (
    AsyncAdaptedConnection,
    DqliteDialect_aio,
)
from sqlalchemydqlite.provision import _format_url


@pytest.mark.parametrize(
    "code",
    [11, 24, 26],  # CORRUPT, FORMAT, NOTADB — the bare-DBE codes
)
def test_async_do_ping_database_error_disconnect_code_returns_false(code: int) -> None:
    dialect = DqliteDialect_aio()
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()

    async def fake_async_ping(_conn: Any) -> None:
        raise DatabaseError("simulated", code=code)

    with (
        patch.object(dialect, "_async_ping", new=fake_async_ping),
        patch("sqlalchemydqlite.aio.await_only", new=lambda c: asyncio.run(c)),
    ):
        result = dialect.do_ping(adapter)

    assert result is False


def test_async_do_ping_database_error_unknown_code_propagates() -> None:
    dialect = DqliteDialect_aio()
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()

    async def fake_async_ping(_conn: Any) -> None:
        raise DatabaseError("simulated", code=999)

    with (
        patch.object(dialect, "_async_ping", new=fake_async_ping),
        patch("sqlalchemydqlite.aio.await_only", new=lambda c: asyncio.run(c)),
        pytest.raises(DatabaseError),
    ):
        dialect.do_ping(adapter)


def test_close_rollback_event_loop_closed_debug_logged_proceeds_to_close(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """has_terminate=True means close must not propagate; the rollback
    "Event loop is closed" arm debug-logs and falls through to close."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "127.0.0.1:9001"
    inner.rollback = AsyncMock(side_effect=RuntimeError("Event loop is closed"))
    inner.close = AsyncMock()
    adapter._connection = inner

    # in_greenlet bypassed so the rollback path runs.
    with (
        patch("sqlalchemydqlite.aio.in_greenlet", return_value=True),
        patch("sqlalchemydqlite.aio.await_only", new=lambda c: asyncio.run(c)),
        caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"),
    ):
        adapter.close()

    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    messages = [r.message for r in debug_records]
    assert any("rollback raised RuntimeError" in m for m in messages), (
        f"expected rollback-side Event loop is closed DEBUG record, got {messages}"
    )


@pytest.mark.parametrize(
    ("input_url", "driver", "expected_drivername"),
    [
        ("dqlite://h:9001/db", None, "dqlite"),
        ("dqlite://h:9001/db", "dqlitedbapi", "dqlite"),
        ("dqlite+aio://h:9001/db", "dqlitedbapi", "dqlite"),
        ("dqlite+aio://h:9001/db", "aio", "dqlite+aio"),
        ("dqlite://h:9001/db", "aio", "dqlite+aio"),
    ],
)
def test_format_url_drivername_dispatch(
    input_url: str,
    driver: str | None,
    expected_drivername: str,
) -> None:
    """``"dqlite"``/``"dqlitedbapi"`` map to bare ``"dqlite"``; any other
    driver to ``"dqlite+driver"``. A ``+dqlitedbapi`` form would mismatch
    SA's dispatch table."""
    parsed = make_url(input_url)
    out = _format_url(parsed, driver, "test1")
    assert out.drivername == expected_drivername


def test_format_url_preserves_host_port_and_embeds_ident() -> None:
    parsed = make_url("dqlite://node-a:9001/db")
    out = _format_url(parsed, None, "ident-x")
    assert out.host == "node-a"
    assert out.port == 9001
    assert out.database is not None
    assert "ident-x" in out.database


def test_format_url_appends_session_token_even_when_ident_is_none() -> None:
    """ident=None drops the follower-ident suffix but still appends the
    session token, so concurrent runs against one cluster don't bleed
    state (dqlite has no DROP DATABASE)."""
    parsed = make_url("dqlite+aio://h:9001/mydb")
    out = _format_url(parsed, "dqlitedbapi", None)
    assert out.drivername == "dqlite"
    assert out.database is not None
    assert out.database.startswith("mydb_")
    assert out.database.startswith("mydb_sa_")  # session-token prefix from provision.py


def test_format_url_does_not_double_append_session_token_in_chained_calls() -> None:
    """SA bootstrap calls ``_format_url`` twice on the same chain; the
    session token must appear at most once, only the follower ident is
    appended on the second pass."""
    from sqlalchemydqlite.provision import _SESSION_TOKEN

    base = make_url("dqlite://h:9001/db")
    step1 = _format_url(base, "dqlitedbapi", None)
    step2 = _format_url(step1, None, "gw0")
    assert step2.database is not None
    assert step2.database.count(_SESSION_TOKEN) == 1
    assert step2.database.endswith("_gw0")


def test_do_begin_emits_begin_through_dbapi_cursor() -> None:
    """``do_begin`` emits ``BEGIN`` via a cursor; that execute path flips
    the client's ``_in_transaction``. Bypassing the cursor would break
    the lifecycle."""
    from sqlalchemydqlite.base import DqliteDialect

    dialect = DqliteDialect()

    fake_cursor = MagicMock()
    fake_conn = MagicMock()
    fake_conn.cursor = MagicMock(return_value=fake_cursor)

    dialect.do_begin(fake_conn)

    fake_cursor.execute.assert_called_once()
    sql = fake_cursor.execute.call_args[0][0]
    assert sql.upper().startswith("BEGIN"), f"expected do_begin to emit BEGIN; got {sql!r}"
