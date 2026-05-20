"""Pin: ``DqliteDialect_aio.connect()`` remaps loop-state RuntimeError
shapes raised by the eager ``await_only(raw_conn.connect())`` arm to
``dbapi.OperationalError`` — the same discipline every other
``await_only`` site in this adapter applies.

The connect path was the sole remaining ``await_only`` site that
did NOT route through ``_handle_exception``. A
cross-loop / closed-loop / nested-loop RuntimeError from
``raw_conn.connect()`` would propagate bare past SA's
``_handle_dbapi_exception`` classifier (gated on ``dbapi.Error``)
and ``engine.dispose()`` would retain the broken slot.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import DqliteDialect_aio


def _make_dialect_with_inner(side_effect: Exception) -> DqliteDialect_aio:
    """Build a dialect whose ``loaded_dbapi.connect()`` returns an
    inner whose async ``.connect()`` raises ``side_effect``."""
    dialect = DqliteDialect_aio.__new__(DqliteDialect_aio)
    inner = MagicMock()
    inner.connect = AsyncMock(side_effect=side_effect)
    inner.address = "localhost:9001"
    inner.terminate = AsyncMock()
    fake_dbapi = MagicMock()
    fake_dbapi.connect = MagicMock(return_value=inner)
    dialect.loaded_dbapi = fake_dbapi
    dialect._validate_connect_kwargs = MagicMock()
    return dialect


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "msg, expected_substring",
    [
        ("Task <foo> attached to a different loop", "event-loop mismatch"),
        ("Task attached to a different event loop", "event-loop mismatch"),
        ("Event loop is closed", "event loop closed"),
        ("This event loop is already running", "event loop already running"),
        ("LOOP IS ALREADY RUNNING", "event loop already running"),
    ],
)
async def test_connect_remaps_loop_state_runtime_error_to_operational_error(
    msg: str, expected_substring: str
) -> None:
    """Each loop-state RuntimeError variant from ``raw_conn.connect()``
    must surface as ``OperationalError`` with the canonical remap
    substring, so SA's pool invalidates the slot rather than letting
    the bare RuntimeError leak past ``_handle_dbapi_exception``."""
    dialect = _make_dialect_with_inner(RuntimeError(msg))

    def _do_connect() -> Any:
        return dialect.connect(host="ignored", port=9001)

    with pytest.raises(OperationalError, match=expected_substring):
        await greenlet_spawn(_do_connect)


@pytest.mark.asyncio
async def test_connect_does_not_remap_unrelated_runtime_error() -> None:
    """A RuntimeError without any loop-state substring must propagate
    bare (matching prior behavior for the non-load-bearing path)."""
    dialect = _make_dialect_with_inner(RuntimeError("some other problem"))

    def _do_connect() -> Any:
        return dialect.connect(host="ignored", port=9001)

    with pytest.raises(RuntimeError, match="some other problem"):
        await greenlet_spawn(_do_connect)


def test_connect_helper_returns_normally_when_no_loop_state_match() -> None:
    """Direct unit test on the remap helper: returns without raising
    when the cause-chain carries no loop-state substring. Callers
    must follow up with their own re-raise."""
    from sqlalchemydqlite.aio import _remap_loop_state_runtime_error

    # Non-matching message — helper returns None without raising.
    _remap_loop_state_runtime_error(RuntimeError("unrelated"))


def test_connect_helper_walks_cause_chain() -> None:
    """An outer exception wrapping a loop-state RuntimeError via
    ``raise X from Y`` must still trigger the remap — the helper
    walks the cause chain. Mirrors the discipline in base.py's
    ``is_disconnect``."""
    from sqlalchemydqlite.aio import _remap_loop_state_runtime_error

    try:
        try:
            raise RuntimeError("Event loop is closed")
        except RuntimeError as inner:
            raise ValueError("wrapper") from inner
    except ValueError as outer:
        with pytest.raises(OperationalError, match="event loop closed"):
            _remap_loop_state_runtime_error(outer)
