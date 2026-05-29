"""dial_timeout and attempt_timeout are reachable from the SA dialect on both the
URL-query and create_engine(connect_args=...) paths."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_url_query_allowlist_includes_dial_and_attempt_timeout() -> None:
    """The URL-query allowlist accepts both keys so they parse without ArgumentError."""
    assert "dial_timeout" in DqliteDialect._URL_QUERY_ALLOWED
    assert "attempt_timeout" in DqliteDialect._URL_QUERY_ALLOWED


def test_connect_kwarg_allowlist_includes_dial_and_attempt_timeout() -> None:
    """The connect_args allowlist mirrors so connect_args={"dial_timeout": 0.5} flows through."""
    assert "dial_timeout" in DqliteDialect._CONNECT_KWARG_ALLOWED
    assert "attempt_timeout" in DqliteDialect._CONNECT_KWARG_ALLOWED


def test_url_dial_timeout_accepts_positive_finite_float() -> None:
    """Positive finite floats round-trip through the URL validator. create_engine
    does not eagerly dial, so the parse succeeding (no ArgumentError) is the pin."""
    engine = create_engine("dqlite://127.0.0.1:9001/db?dial_timeout=0.5&attempt_timeout=1.0")
    assert engine.dialect.driver == "dqlitedbapi"
    engine.dispose()


def test_url_dial_timeout_rejects_zero() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?dial_timeout=0")


def test_url_attempt_timeout_rejects_negative() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?attempt_timeout=-1.0")


def test_url_dial_timeout_rejects_non_finite() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?dial_timeout=inf")


def test_url_typo_in_dial_timeout_key_still_rejected() -> None:
    """A typo must still raise — the new key must not loosen the closed allowlist."""
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?dial_timout=0.5")
