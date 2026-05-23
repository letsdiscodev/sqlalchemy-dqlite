"""Pin: ``isolation_level`` in ``connect_args`` (non-AUTOCOMMIT)
raises ``ArgumentError`` with a directional message pointing the user
at the engine-level kwarg, not the generic "Check ``connect_args=``
for typos" message.

The dialect routes ``isolation_level`` via
``create_engine(isolation_level=...)`` only — the prior generic
diagnostic misled users into chasing a typo when the kwarg is
legitimate but on the wrong layer.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_isolation_level_serializable_in_connect_args_directional_message() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="create_engine"):
        dialect._validate_connect_kwargs({"isolation_level": "SERIALIZABLE"})


def test_isolation_level_autocommit_still_uses_dedicated_message() -> None:
    """The pre-existing AUTOCOMMIT special-case still fires first
    with its dedicated rejection message — not the generic
    ``create_engine`` redirect."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        dialect._validate_connect_kwargs({"isolation_level": "AUTOCOMMIT"})


def test_no_kwargs_still_passes() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({})
