"""isolation_level in connect_args raises ArgumentError pointing at the
engine-level kwarg (it routes via create_engine only), not the generic
typo message."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_isolation_level_serializable_in_connect_args_directional_message() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="create_engine"):
        dialect._validate_connect_kwargs({"isolation_level": "SERIALIZABLE"})


def test_isolation_level_autocommit_still_uses_dedicated_message() -> None:
    """AUTOCOMMIT still fires its dedicated message, not the create_engine
    redirect."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        dialect._validate_connect_kwargs({"isolation_level": "AUTOCOMMIT"})


def test_no_kwargs_still_passes() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({})
