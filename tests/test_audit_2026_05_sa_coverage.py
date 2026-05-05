"""Coverage gaps surfaced by the 2026-05 SA-side audit pass.

Four behavioural arms that exist correctly in the source but were
not pinned by the existing test suite — a regression in any one
would have shipped silently:

1. ``DqliteDialect_aio.do_ping`` async DatabaseError-with-disconnect-
   code arm (CORRUPT/FORMAT/NOTADB → False; unknown code → re-raise).
2. ``AsyncAdaptedConnection.close`` rollback-side
   ``RuntimeError("Event loop is closed")`` debug-log arm.
3. ``provision._format_url`` parametrised over multiple URL shapes
   to lock URL fan-out semantics (with/without +driver suffix,
   with/without ident).
4. SA ``do_begin`` keeps the underlying client-layer
   ``_in_transaction`` flag in sync — pin via a unit-level proxy
   that asserts the BEGIN trip flips the flag through the adapter
   chain.
"""

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

# ---------------------------------------------------------------
# 1. do_ping DatabaseError disconnect-codes arm
# ---------------------------------------------------------------


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


# ---------------------------------------------------------------
# 2. AsyncAdaptedConnection.close rollback "Event loop is closed"
# ---------------------------------------------------------------


def test_close_rollback_event_loop_closed_debug_logged_proceeds_to_close(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """During engine.dispose() after asyncio.run() torn down its
    loop, the rollback await raises RuntimeError("Event loop is
    closed"). The has_terminate=True dialect promise says close
    must NOT propagate, so this arm debug-logs and falls through
    to the close arm in the finally."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner = MagicMock()
    inner.address = "127.0.0.1:9001"
    inner.rollback = AsyncMock(side_effect=RuntimeError("Event loop is closed"))
    inner.close = AsyncMock()
    adapter._connection = inner

    # Drive close() with the in_greenlet preflight bypassed so the
    # rollback path runs.
    with (
        patch("sqlalchemydqlite.aio.in_greenlet", return_value=True),
        patch("sqlalchemydqlite.aio.await_only", new=lambda c: asyncio.run(c)),
        caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"),
    ):
        # No raise; the arm absorbs and proceeds to close.
        adapter.close()

    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    messages = [r.message for r in debug_records]
    assert any("rollback raised RuntimeError" in m for m in messages), (
        f"expected rollback-side Event loop is closed DEBUG record, got {messages}"
    )


# ---------------------------------------------------------------
# 3. provision._format_url URL fan-out parametrisation
# ---------------------------------------------------------------


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
    """``_format_url`` maps ``"dqlite"`` and ``"dqlitedbapi"`` driver
    tokens to the bare ``"dqlite"`` drivername, and any other
    driver to ``"dqlite+driver"``. Pinning here against future
    refactors that might silently route ``"dqlitedbapi"`` to a
    ``+dqlitedbapi`` form (which would mismatch SA's dispatch
    table)."""
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
    """ident=None means no follower-ident suffix, but the
    session-unique token IS still appended so concurrent pytest runs
    against the same cluster do not bleed state across runs (dqlite
    has no DROP DATABASE primitive). Used by
    ``_dqlite_generate_driver_url`` which only flips sync↔async,
    not the database identity."""
    parsed = make_url("dqlite+aio://h:9001/mydb")
    out = _format_url(parsed, "dqlitedbapi", None)
    assert out.drivername == "dqlite"
    assert out.database is not None
    assert out.database.startswith("mydb_")
    # No follower-ident substring appended.
    assert out.database.startswith("mydb_sa_")  # session-token prefix from provision.py


# ---------------------------------------------------------------
# 4. do_begin keeps client-layer _in_transaction in sync
# ---------------------------------------------------------------


def test_do_begin_emits_begin_through_dbapi_cursor() -> None:
    """Pin: ``DqliteDialect.do_begin`` emits ``BEGIN`` via a cursor
    on the dbapi connection. The dbapi cursor's execute path
    invokes the client's ``_update_tx_flags_from_sql`` which flips
    ``_in_transaction``. A future SA refactor that bypasses the
    cursor (e.g. opening a savepoint directly via the dbapi
    connection.execute shortcut without going through ``BEGIN``
    first) would silently break the lifecycle.

    Unit-level pin: assert the dialect's ``do_begin`` issues
    ``BEGIN`` SQL — without enforcing this, the wire-level
    transaction state could drift from SA's expectations.
    """
    from sqlalchemydqlite.base import DqliteDialect

    dialect = DqliteDialect()

    # Build a stub dbapi-connection chain. The cursor receives
    # ``BEGIN`` via execute().
    fake_cursor = MagicMock()
    fake_conn = MagicMock()
    fake_conn.cursor = MagicMock(return_value=fake_cursor)

    dialect.do_begin(fake_conn)

    fake_cursor.execute.assert_called_once()
    sql = fake_cursor.execute.call_args[0][0]
    assert sql.upper().startswith("BEGIN"), f"expected do_begin to emit BEGIN; got {sql!r}"
