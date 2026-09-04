"""``DqliteDialect_aio.connect`` rejects a non-callable ``async_creator_fn``
with ``ArgumentError`` at connect-time, not a bare TypeError deep in dispatch."""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio


@pytest.mark.parametrize("bogus", [42, "string", object(), [1, 2]])
def test_async_creator_fn_noncallable_raises_argument_error(bogus: Any) -> None:
    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        dialect.connect(async_creator_fn=bogus)
