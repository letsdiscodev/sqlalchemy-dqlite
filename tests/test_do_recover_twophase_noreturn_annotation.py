"""``DqliteDialect.do_recover_twophase`` is a raise-only stub (NotSupportedError)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import NotSupportedError
from sqlalchemydqlite.base import DqliteDialect


def test_do_recover_twophase_still_raises_not_supported_error() -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    with pytest.raises(NotSupportedError, match="two-phase"):
        dialect.do_recover_twophase(MagicMock())
