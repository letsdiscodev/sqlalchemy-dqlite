"""``AsyncAdaptedConnection`` must declare ``__slots__`` to preserve
the memory layout of SA's parent ``AdaptedConnection`` (which uses
slots for every pooled connection). Without slots each instance
silently gets a ``__dict__``, defeating the optimization.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_async_adapted_connection_has_slots() -> None:
    conn = AsyncAdaptedConnection(MagicMock())
    with pytest.raises(AttributeError):
        conn.__dict__  # noqa: B018
