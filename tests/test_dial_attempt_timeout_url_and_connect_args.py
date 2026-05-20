"""Pin: ``dial_timeout`` and ``attempt_timeout`` are reachable from
the SA dialect on both the URL-query path and the
``create_engine(connect_args=...)`` path.

The dbapi layer surfaces both knobs (mirroring go-dqlite's
``Config.DialTimeout`` / ``Config.AttemptTimeout``); without this
SA-side propagation, SA URL parsing rejects ``?dial_timeout=0.5``
with ``ArgumentError`` ("Invalid query keys") and operators have
no way to thread the parity knobs through the SA layer.

The pin asserts (a) both keys are in the URL-query allowlist with
the expected validators, and (b) both keys are in the
``connect_args=`` allowlist so a typo ``?dial_timout=0.5`` still
trips the URL-time ``ArgumentError`` while the legitimate spelling
flows through.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_url_query_allowlist_includes_dial_and_attempt_timeout() -> None:
    """The URL-query allowlist must accept both keys so
    ``create_engine("dqlite://host:port/db?dial_timeout=0.5&attempt_timeout=1.0")``
    parses without ArgumentError."""
    assert "dial_timeout" in DqliteDialect._URL_QUERY_ALLOWED
    assert "attempt_timeout" in DqliteDialect._URL_QUERY_ALLOWED


def test_connect_kwarg_allowlist_includes_dial_and_attempt_timeout() -> None:
    """The connect_args allowlist must mirror so
    ``create_engine(..., connect_args={"dial_timeout": 0.5})``
    flows through without the first-checkout NotSupportedError."""
    assert "dial_timeout" in DqliteDialect._CONNECT_KWARG_ALLOWED
    assert "attempt_timeout" in DqliteDialect._CONNECT_KWARG_ALLOWED


def test_url_dial_timeout_accepts_positive_finite_float() -> None:
    """Positive finite floats round-trip through the URL validator
    without raising. ``create_engine`` itself does not eagerly dial,
    so the URL parse succeeding (no ArgumentError) is the relevant
    pin."""
    engine = create_engine("dqlite://127.0.0.1:9001/db?dial_timeout=0.5&attempt_timeout=1.0")
    # No ArgumentError raised — the URL parse accepted both keys.
    assert engine.dialect.driver == "dqlitedbapi"
    engine.dispose()


def test_url_dial_timeout_rejects_zero() -> None:
    """The validator rejects zero (matches the timeout validator)."""
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?dial_timeout=0")


def test_url_attempt_timeout_rejects_negative() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?attempt_timeout=-1.0")


def test_url_dial_timeout_rejects_non_finite() -> None:
    """``inf`` and ``nan`` strings are not legitimate dial budgets."""
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?dial_timeout=inf")


def test_url_typo_in_dial_timeout_key_still_rejected() -> None:
    """A typo (``dial_timout``) must still raise — adding the new
    key must not loosen the closed-allowlist contract."""
    with pytest.raises(ArgumentError):
        create_engine("dqlite://127.0.0.1:9001/db?dial_timout=0.5")
