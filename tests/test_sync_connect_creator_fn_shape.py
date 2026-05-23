"""Pin: sync ``DqliteDialect.connect`` rejects non-callable
``creator_fn`` and async-def ``creator_fn`` with a precise
``ArgumentError``. Mirrors the async sibling's discipline at
``aio.py`` (which rejects sync ``async_creator_fn``).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def _make_dialect() -> DqliteDialect:
    return DqliteDialect()


def test_creator_fn_non_callable_rejected() -> None:
    dialect = _make_dialect()
    with pytest.raises(ArgumentError, match="must be callable"):
        dialect.connect("host:9001", creator_fn="not-callable")


def test_creator_fn_async_def_rejected_with_directional_message() -> None:
    dialect = _make_dialect()

    async def async_creator(*_args: Any, **_kwargs: Any) -> Any:
        return None

    with pytest.raises(ArgumentError, match="dqlite\\+aio://"):
        dialect.connect("host:9001", creator_fn=async_creator)


def test_creator_fn_callable_synchronous_accepted() -> None:
    dialect = _make_dialect()

    seen: dict[str, object] = {}

    def fake_creator(address: str, **kwargs: Any) -> object:
        seen["address"] = address
        return "sentinel-connection"

    result = dialect.connect("host:9001", creator_fn=fake_creator)
    assert result == "sentinel-connection"
    assert seen["address"] == "host:9001"


# Quiet unused-asyncio import.
_ = asyncio
