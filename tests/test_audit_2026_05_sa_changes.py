"""Behavioural pins for the 2026-05 SA-side audit changes.

Each test guards a contract added or sharpened in a specific issue:

- ``DqliteDialect.__init__`` rejects ``native_datetime`` kwarg.
- ``AsyncAdaptedCursor.connection`` raises ``InterfaceError`` on a
  closed cursor instead of returning the post-close weakref proxy.
- ``DqliteDialect.connect`` honours a sync ``creator_fn``.
- ``DqliteDialect_aio._handle_exception`` remaps
  ``RuntimeError("This event loop is already running")`` to a
  disconnect-classified ``OperationalError``.
- URL ``?close_timeout=0.0001`` is rejected by the validator.
- URL ``?max_total_rows=none`` emits a one-shot WARNING.
- ``provision._dqlite_run_reap_dbs`` forces sync drivername even when
  the input URL was ``dqlite+aio://``.
"""

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

from dqlitedbapi.exceptions import InterfaceError, OperationalError
from sqlalchemydqlite.aio import (
    AsyncAdaptedConnection,
    AsyncAdaptedCursor,
    DqliteDialect_aio,
)
from sqlalchemydqlite.base import DqliteDialect
from sqlalchemydqlite.provision import _format_url


def test_dialect_init_rejects_native_datetime_kwarg() -> None:
    with pytest.raises(ArgumentError, match="native_datetime"):
        DqliteDialect(native_datetime=True)
    with pytest.raises(ArgumentError, match="native_datetime"):
        DqliteDialect(native_datetime=False)  # also rejected — semantics don't apply at all


def test_async_adapted_cursor_connection_property_raises_after_close() -> None:
    """Pin: cursor.connection on a closed cursor must raise
    InterfaceError, NOT return the post-close weakref proxy. A
    GC'd parent through a proxy would surface as bare
    ReferenceError that escapes the dbapi.Error hierarchy."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    cur = AsyncAdaptedCursor(adapter)

    # Pre-close: connection property returns the parent.
    assert cur.connection is adapter

    cur.close()

    with pytest.raises(InterfaceError, match="cursor is closed"):
        _ = cur.connection


def test_sync_dialect_connect_honours_creator_fn() -> None:
    dialect = DqliteDialect()
    dialect.loaded_dbapi = MagicMock()  # would-be default factory

    called: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    sentinel = object()

    def fake_creator(*args: Any, **kwargs: Any) -> object:
        called.append((args, kwargs))
        return sentinel

    result = dialect.connect("1.2.3.4:9000", database="mydb", creator_fn=fake_creator)

    assert result is sentinel
    assert called == [(("1.2.3.4:9000",), {"database": "mydb"})]
    dialect.loaded_dbapi.connect.assert_not_called()


def test_sync_dialect_connect_creator_fn_pop_precedes_allowlist() -> None:
    """If the creator_fn pop didn't precede _validate_connect_kwargs,
    the strict allowlist would reject the hook key with ArgumentError
    before the creator gets a chance."""
    dialect = DqliteDialect()
    dialect.loaded_dbapi = MagicMock()

    sentinel = object()
    # No allowlist failure — only succeeds if pop runs before validate.
    result = dialect.connect(
        "1.2.3.4:9000",
        database="mydb",
        creator_fn=lambda *_a, **_kw: sentinel,
    )
    assert result is sentinel


def test_sync_dialect_connect_unknown_kwarg_still_rejected() -> None:
    dialect = DqliteDialect()
    dialect.loaded_dbapi = MagicMock()
    with pytest.raises(ArgumentError):
        dialect.connect("h:1", database="mydb", unknown_kwarg=True)


def test_async_handle_exception_remaps_loop_already_running() -> None:
    """RuntimeError('This event loop is already running') must be
    remapped to OperationalError so SA's is_disconnect classifier
    (gated on DatabaseError) catches it via the substring scan."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()

    err = RuntimeError("This event loop is already running")
    with pytest.raises(OperationalError, match="loop is already running"):
        adapter._handle_exception(err)


def test_is_disconnect_matches_loop_is_already_running_substring() -> None:
    """The remapped wording 'event loop already running: ...' must
    fall under the substring scan in _dqlite_disconnect_messages —
    closing the loop where the remap site and the classifier diverge."""
    dialect = DqliteDialect_aio()
    err = OperationalError(
        "event loop already running: This event loop is already running",
        code=None,
    )
    assert dialect.is_disconnect(err, connection=None, cursor=None) is True


def test_url_close_timeout_below_floor_rejected() -> None:
    dialect = DqliteDialect_aio()
    url = make_url("dqlite://h:9001/db?close_timeout=0.0001")
    with pytest.raises(ArgumentError, match="close_timeout"):
        dialect.create_connect_args(url)


def test_url_close_timeout_at_floor_accepted() -> None:
    dialect = DqliteDialect_aio()
    url = make_url("dqlite://h:9001/db?close_timeout=0.01")
    args, kwargs = dialect.create_connect_args(url)
    assert kwargs["close_timeout"] == 0.01


def test_url_max_total_rows_none_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Pin: when max_total_rows resolves to None via the URL, a
    one-shot WARNING is emitted — operator-UX hint that they've
    disabled the row-count cap."""
    # Reset the one-shot gate so this test sees the emit even if a
    # prior test in the same process tripped it.
    DqliteDialect._max_total_rows_disabled_warning_emitted = False
    DqliteDialect_aio._max_total_rows_disabled_warning_emitted = False

    dialect = DqliteDialect_aio()
    url = make_url("dqlite://h:9001/db?max_total_rows=none")
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        dialect.create_connect_args(url)

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("max_total_rows" in r.message for r in warnings), (
        f"expected max_total_rows warning, got: {[r.message for r in warnings]}"
    )

    # One-shot: a second call must NOT emit again.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        dialect.create_connect_args(url)
    warnings_after = [r for r in caplog.records if r.levelname == "WARNING"]
    assert not warnings_after, "expected one-shot WARNING; got duplicate"


def test_provision_format_url_forces_sync_drivername_when_input_is_aio() -> None:
    """Pin: ``_format_url(parsed, "dqlitedbapi", ident)`` returns a
    URL with the bare ``dqlite`` drivername regardless of whether
    the input was ``dqlite://`` or ``dqlite+aio://``. This is what
    keeps ``_dqlite_run_reap_dbs`` routed to the sync dialect even
    when the test suite was driven by an async engine."""
    aio_input = make_url("dqlite+aio://h:9001/db")
    rewritten = _format_url(aio_input, "dqlitedbapi", "test1")
    assert rewritten.drivername == "dqlite"  # not "dqlite+aio" / "dqlite+dqlitedbapi"
    # Database name is built from the ident with a workspace-prefix
    # suffix; the ident must be embedded.
    assert rewritten.database is not None and "test1" in rewritten.database

    sync_input = make_url("dqlite://h:9001/db")
    rewritten_sync = _format_url(sync_input, "dqlitedbapi", "test1")
    assert rewritten_sync.drivername == "dqlite"
    assert rewritten_sync.database is not None and "test1" in rewritten_sync.database
