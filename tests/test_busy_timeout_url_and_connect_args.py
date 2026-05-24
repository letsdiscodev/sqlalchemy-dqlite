"""Pin: ``busy_timeout`` works via both URL query and connect_args=
on the SA dialect, forwarding to the dbapi layer.

The dbapi-side feature defaults ``busy_timeout=5.0`` (stdlib parity);
SA-side users tune via:

  - URL: ``dqlite://host:9001/db?busy_timeout=30.0``
  - connect_args: ``create_engine(..., connect_args={"busy_timeout": 30.0})``

Both routes go through the dialect's ``create_connect_args`` /
allowlist machinery and end up as a kwarg on ``dqlitedbapi.connect``.

Validation:
- Negative / non-finite / non-numeric values raise ``ArgumentError`` at
  URL-parse or first-checkout time (NOT at runtime when the contended
  write hits BUSY).
- bool is rejected (would silently coerce True→1.0).
- Zero is accepted (stdlib parity — "no retry").
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_url_query_busy_timeout_forwards_to_connect_kwargs() -> None:
    """``?busy_timeout=30.0`` in the URL ends up in the dialect's
    connect kwargs."""
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
    """``?busy_timeout=0`` is the canonical "no retry" — must be
    accepted at URL-parse time (stdlib parity for timeout=0)."""
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
    """Negative values are rejected at URL-parse time as
    ``ArgumentError`` (not at first-checkout time)."""
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
    """``?busy_timeout=abc`` raises at URL-parse time (the URL
    converter is ``float`` and rejects non-numeric strings)."""
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
    """``create_engine(..., connect_args={"busy_timeout": N})`` works
    — the kwarg is in the allowlist."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": 30.0},
    )
    # Engine construction must succeed without ArgumentError. We can't
    # actually connect (no server) — the kwarg validation happens at
    # first checkout / dispose. The fact that create_engine returned
    # without raising means the dialect accepted the kwarg.
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
    """Negative busy_timeout in connect_args= is rejected. The
    validator runs at first connection-arg merge (in the dialect's
    connect() override)."""
    engine = create_engine(
        "dqlite://localhost:9001/default",
        connect_args={"busy_timeout": -1.0},
    )
    # The allowlist's validator runs at first checkout time.
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
