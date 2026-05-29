"""``AsyncAdaptedConnection`` must declare ``__slots__`` to preserve the parent
``AdaptedConnection``'s slotted layout; without it instances silently gain a ``__dict__``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_async_adapted_connection_has_slots() -> None:
    conn = AsyncAdaptedConnection(MagicMock())
    with pytest.raises(AttributeError):
        conn.__dict__  # noqa: B018
