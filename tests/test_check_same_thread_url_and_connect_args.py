"""SA dialect accepts ``check_same_thread`` via both URL query and
connect_args=, forwarding to ``dqlitedbapi.connect()`` (stdlib sqlite3
parity). connect_args requires a strict bool; URL rejects bad tokens."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_url_query_check_same_thread_false_forwards_to_kwargs() -> None:
    """``?check_same_thread=false`` parses to the kwarg."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "false"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is False


def test_url_query_check_same_thread_true_forwards_to_kwargs() -> None:
    """``?check_same_thread=true`` parses to the kwarg."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "true"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is True


@pytest.mark.parametrize(
    "token,expected",
    [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("1", True),
        ("yes", True),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        ("0", False),
        ("no", False),
    ],
)
def test_url_query_bool_tokens_accepted(token: str, expected: bool) -> None:
    """The URL bool parser accepts the standard token set
    case-insensitively (shared ``_parse_url_bool`` helper)."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": token},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is expected


def test_url_query_invalid_token_rejected() -> None:
    """``?check_same_thread=notabool`` raises at URL-parse time."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "notabool"},
    )
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_connect_args_check_same_thread_false_accepted() -> None:
    """``connect_args={"check_same_thread": False}`` works."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": False},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_check_same_thread_true_accepted() -> None:
    """Explicit ``True`` also works (no-op vs default)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": True},
    )
    assert engine is not None
    engine.dispose()


def test_connect_args_check_same_thread_rejects_int_zero() -> None:
    """Strict bool: ``0`` is not ``False`` for this kwarg
    (mirrors the dbapi-side strict-bool validation)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": 0},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_connect_args_check_same_thread_rejects_string() -> None:
    """Strict bool: string ``"false"`` rejected via the dbapi
    surface (the SA validator catches non-bool too)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"check_same_thread": "false"},
    )
    with pytest.raises(ArgumentError):
        engine.connect()
    engine.dispose()


def test_url_query_combined_with_other_knobs() -> None:
    """Combine check_same_thread with other URL knobs (busy_timeout)
    to confirm they don't interfere."""
    url = URL.create(
        "dqlite",
        host="localhost",
        port=9001,
        database="default",
        query={"check_same_thread": "false", "busy_timeout": "10.0"},
    )
    dialect = DqliteDialect()
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("check_same_thread") is False
    assert kwargs.get("busy_timeout") == 10.0
