"""Pin: ``_DqliteTime.bind_processor`` returns a callable (not ``None``, the previous SA
bind-side opt-out)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteTime


def test_bind_processor_returns_callable_for_default_dialect() -> None:
    dialect: Any = MagicMock()
    proc = _DqliteTime().bind_processor(dialect)
    assert callable(proc)


def test_bind_processor_returns_callable_for_timezone_true_variant() -> None:
    dialect: Any = MagicMock()
    proc = _DqliteTime(timezone=True).bind_processor(dialect)
    assert callable(proc)
