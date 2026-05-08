"""Pin: ``connect_args={...}`` per-key values are validated against the
same per-key validators that the URL-query path enforces.

ISSUE-DT4 closed the unknown-key asymmetry but left the value-range
asymmetry open: ``_URL_QUERY_ALLOWED`` carries per-key validators
(e.g. ``close_timeout`` floor 0.01s) that ``_validate_connect_kwargs``
was not invoking. This test pins that the validator now runs against
``connect_args`` values too.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_connect_args_close_timeout_below_floor_raises() -> None:
    """``close_timeout=0.0001`` violates the URL-query 0.01s floor."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="close_timeout"):
        dialect._validate_connect_kwargs({"close_timeout": 0.0001})


def test_connect_args_close_timeout_at_floor_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"close_timeout": 0.01})


def test_connect_args_close_timeout_above_floor_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"close_timeout": 5.0})


def test_connect_args_timeout_zero_raises() -> None:
    """``timeout`` validator requires ``> 0`` and ``isfinite``."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="timeout"):
        dialect._validate_connect_kwargs({"timeout": 0.0})


def test_connect_args_timeout_inf_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="timeout"):
        dialect._validate_connect_kwargs({"timeout": float("inf")})


def test_connect_args_timeout_positive_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"timeout": 5.0})


def test_connect_args_unknown_key_still_raises() -> None:
    """Regression guard: the original allowlist check still fires."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="Unknown"):
        dialect._validate_connect_kwargs({"not_a_known_key": 1})
