"""``busy_timeout`` works via both URL query and connect_args= on the SA
dialect, forwarding to the dbapi layer. Bad values raise ``ArgumentError``
at parse/checkout time, not at runtime BUSY."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_url_query_busy_timeout_forwards_to_connect_kwargs() -> None:
    """``?busy_timeout=30.0`` ends up in the dialect's connect kwargs."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "30.0"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("busy_timeout") == 30.0


def test_url_query_busy_timeout_zero_accepted() -> None:
    """``?busy_timeout=0`` ("no retry") is accepted (stdlib parity)."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "0"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("busy_timeout") == 0.0


def test_url_query_busy_timeout_negative_rejected() -> None:
    """Negative values raise ``ArgumentError`` at URL-parse time."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "-1"},
    )
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_url_query_busy_timeout_non_numeric_rejected() -> None:
    """``?busy_timeout=abc`` raises at URL-parse time (converter is
    ``float``)."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"busy_timeout": "abc"},
    )
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_connect_args_busy_timeout_accepted() -> None:
    """``connect_args={"busy_timeout": N}`` works — kwarg is allowlisted."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": 30.0},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_busy_timeout_zero_accepted() -> None:
    """Zero accepted via connect_args= same as URL."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": 0.0},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_busy_timeout_negative_rejected() -> None:
    """Negative busy_timeout in connect_args= is rejected at first
    checkout (validator runs in the dialect's connect() override)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": -1.0},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_connect_args_busy_timeout_bool_rejected() -> None:
    """bool is rejected (would silently coerce True→1.0)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": True},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()
