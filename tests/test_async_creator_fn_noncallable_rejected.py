"""Pin: ``DqliteDialect_aio.connect`` rejects a non-callable
``async_creator_fn`` with ``ArgumentError`` — symmetric with the
sync sibling that already rejects both async-def and non-callable.

Pre-fix the async dialect rejected ``async def`` creator_fn but
passed non-callables through to ``creator_fn(*cargs, **cparams)``
which raised a bare ``TypeError("'X' object is not callable")``
far from the configuration site.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio


@pytest.mark.parametrize("bogus", [42, "string", object(), None, [1, 2]])
def test_async_creator_fn_noncallable_raises_argument_error(bogus: Any) -> None:
    """Non-callable async_creator_fn must raise ArgumentError at
    connect-time so the operator sees the diagnostic at the
    configuration site, not deep in the dispatch chain."""
    if bogus is None:
        pytest.skip("None means 'no creator_fn'; the gate correctly does not raise.")
    dialect = DqliteDialect_aio()
    with pytest.raises(ArgumentError, match="async_creator_fn"):
        # The connect method consumes async_creator_fn from cparams.
        dialect.connect(async_creator_fn=bogus)
