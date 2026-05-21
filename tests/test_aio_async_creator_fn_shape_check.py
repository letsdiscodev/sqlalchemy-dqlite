"""Pin: ``DqliteDialect_aio.connect`` rejects ``async_creator_fn``
defined as ``async def`` (or a coroutine-function wrapper) with a
clear ``ArgumentError`` at connect time.

The hook is called SYNCHRONOUSLY and the returned object's
``connect()`` is then awaited. A user writing ``async def my_creator``
returns a coroutine from the sync call, not an ``AsyncConnection``-
shape object; the downstream ``raw_conn.connect()`` would raise
``AttributeError: 'coroutine' object has no attribute 'connect'``
far from the user's config site. Pre-flight check moves the
diagnostic to ``connect`` time.

Use ``asyncio.iscoroutinefunction`` (not ``inspect``) to also catch
``functools.partial`` wrappers around an async def.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio


def test_async_def_creator_fn_raises_argument_error() -> None:
    """The canonical misuse: ``async def`` creator. The diagnostic
    must fire at ``connect`` time, naming ``async_creator_fn``."""

    async def bad_creator(**_: Any) -> Any:  # noqa: RUF029
        return None

    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=bad_creator)


def test_partial_wrapping_async_def_creator_also_rejected() -> None:
    """``asyncio.iscoroutinefunction`` recognises
    ``functools.partial(async_def_func, ...)`` (where
    ``inspect.iscoroutinefunction`` would return False). A user
    wrapping their async creator with partial must also see the
    diagnostic."""

    async def inner_creator(addr: str, **_: Any) -> Any:  # noqa: RUF029
        return None

    wrapper = functools.partial(inner_creator, "127.0.0.1:9001")

    # Sanity: stdlib agrees this is a coroutine function.
    assert inspect.iscoroutinefunction(wrapper)

    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=wrapper)


def test_sync_creator_passes_shape_check() -> None:
    """A regular sync callable does NOT trip the pre-flight reject;
    the connection then fails at the downstream call (we don't have
    a live cluster) which is fine for this test — we only need to
    prove the pre-flight ArgumentError is not the raise we see."""

    def sync_creator(**_: Any) -> Any:
        raise RuntimeError("downstream sentinel")

    dialect = DqliteDialect_aio()
    # The pre-flight ArgumentError check must NOT fire on a sync
    # callable. The downstream failure mode (sentinel) is what we
    # observe.
    with pytest.raises(RuntimeError, match="downstream sentinel"):
        dialect.connect(async_creator_fn=sync_creator)
