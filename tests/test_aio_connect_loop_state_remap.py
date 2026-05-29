"""Pin: ``connect()`` remaps loop-state RuntimeError from ``raw_conn.connect()`` to
``OperationalError`` so SA's disconnect classifier (gated on ``dbapi.Error``) evicts the slot."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.util import greenlet_spawn

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import DqliteDialect_aio


def _make_dialect_with_inner(side_effect: Exception) -> DqliteDialect_aio:
    """Dialect whose inner async ``.connect()`` raises ``side_effect``."""
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
    """Each loop-state RuntimeError surfaces as OperationalError with the remap substring."""
    dialect = _make_dialect_with_inner(RuntimeError(msg))

    def _do_connect() -> Any:
        return dialect.connect(host="ignored", port=9001)

    with pytest.raises(OperationalError, match=expected_substring):
        await greenlet_spawn(_do_connect)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "msg, expected_substring",
    [
        ("AsyncConnection used from a different event loop", "event-loop mismatch"),
        ("This event loop is closed (id=42)", "event loop closed"),
    ],
)
async def test_connect_remaps_loop_state_programming_error_to_operational_error(
    msg: str, expected_substring: str
) -> None:
    """Cross-loop reuse raises ProgrammingError (not RuntimeError); the helper filter catches it."""
    from dqlitedbapi.exceptions import ProgrammingError as _DbapiProgrammingError

    dialect = _make_dialect_with_inner(_DbapiProgrammingError(msg))

    def _do_connect() -> Any:
        return dialect.connect(host="ignored", port=9001)

    with pytest.raises(OperationalError, match=expected_substring):
        await greenlet_spawn(_do_connect)


@pytest.mark.asyncio
async def test_connect_does_not_remap_unrelated_runtime_error() -> None:
    """A RuntimeError without a loop-state substring must propagate bare."""
    dialect = _make_dialect_with_inner(RuntimeError("some other problem"))

    def _do_connect() -> Any:
        return dialect.connect(host="ignored", port=9001)

    with pytest.raises(RuntimeError, match="some other problem"):
        await greenlet_spawn(_do_connect)


def test_connect_helper_returns_normally_when_no_loop_state_match() -> None:
    """Helper returns without raising when the cause-chain has no loop-state substring."""
    from sqlalchemydqlite.aio import _remap_loop_state_runtime_error

    _remap_loop_state_runtime_error(RuntimeError("unrelated"))


def test_connect_helper_walks_cause_chain() -> None:
    """An outer exception wrapping a loop-state RuntimeError via ``from`` must still remap."""
    from sqlalchemydqlite.aio import _remap_loop_state_runtime_error

    try:
        try:
            raise RuntimeError("Event loop is closed")
        except RuntimeError as inner:
            raise ValueError("wrapper") from inner
    except ValueError as outer:
        with pytest.raises(OperationalError, match="event loop closed"):
            _remap_loop_state_runtime_error(outer)
