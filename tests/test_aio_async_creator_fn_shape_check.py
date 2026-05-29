"""``connect`` rejects an ``async def`` (or coroutine-function) ``async_creator_fn`` with a
clear ``ArgumentError``, since the hook is called synchronously and its result is awaited.

Uses ``asyncio.iscoroutinefunction`` (not ``inspect``) to also catch ``functools.partial``.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio


def test_async_def_creator_fn_raises_argument_error() -> None:

    async def bad_creator(**_: Any) -> Any:  # noqa: RUF029
        return None

    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=bad_creator)


def test_partial_wrapping_async_def_creator_also_rejected() -> None:
    """``asyncio.iscoroutinefunction`` recognises ``partial`` around an async def
    (where ``inspect.iscoroutinefunction`` returns False)."""

    async def inner_creator(addr: str, **_: Any) -> Any:  # noqa: RUF029
        return None

    wrapper = functools.partial(inner_creator, "127.0.0.1:9001")

    assert inspect.iscoroutinefunction(wrapper)

    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=wrapper)


def test_sync_creator_passes_shape_check() -> None:
    """A sync callable does not trip the pre-flight reject; we observe the downstream failure."""

    def sync_creator(**_: Any) -> Any:
        raise RuntimeError("downstream sentinel")

    dialect = DqliteDialect_aio()
    with pytest.raises(RuntimeError, match="downstream sentinel"):
        dialect.connect(async_creator_fn=sync_creator)
