"""Pin: ``max_message_size`` flows from the SA URL query string AND
from ``connect_args=`` into the dbapi layer's
``Connection`` / ``AsyncConnection`` ``_max_message_size`` slot.

The companion dbapi-side round-trip pin is at
``python-dqlite-dbapi/tests/test_max_message_size_round_trip.py``;
this file covers the two SA surfaces.

Validation lives at the wire layer (the dbapi forward does NOT
re-validate). The SA URL converter coerces the string to int and
gates the URL-time range check (positive 32-bit signed); the
``connect_args=`` path skips the converter and runs the validator
against the typed value the caller supplied.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import create_async_engine


def _extract_connect_args(engine: object) -> dict[str, object]:
    # SA's ``Engine.dialect.create_connect_args(url)`` returns the
    # final positional/keyword arg pair the dialect would pass to
    # ``dbapi.connect()``. Inspect the kwargs to verify the URL
    # query / connect_args propagated.
    args, kwargs = engine.dialect.create_connect_args(engine.url)  # type: ignore[attr-defined]
    return dict(kwargs)


def test_url_query_max_message_size_propagates() -> None:
    engine = create_engine(
        "dqlite://localhost:9001/test?max_message_size=12345",
    )
    try:
        kwargs = _extract_connect_args(engine)
        assert kwargs.get("max_message_size") == 12345
    finally:
        engine.dispose()


def test_url_query_max_message_size_none_propagates() -> None:
    """``?max_message_size=none`` is the documented sentinel for "use
    wire default" — must propagate as ``None`` (the dbapi sentinel)."""
    engine = create_engine(
        "dqlite://localhost:9001/test?max_message_size=none",
    )
    try:
        kwargs = _extract_connect_args(engine)
        assert kwargs.get("max_message_size") is None
    finally:
        engine.dispose()


def test_url_query_max_message_size_negative_rejected() -> None:
    """Negative values rejected at URL-parse time as ``ArgumentError``
    so the typo surfaces at engine construction, not at first
    checkout.
    """
    with pytest.raises(ArgumentError):
        create_engine("dqlite://localhost:9001/test?max_message_size=-1")


def test_url_query_max_message_size_non_int_rejected() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://localhost:9001/test?max_message_size=abc")


def test_connect_args_max_message_size_propagates() -> None:
    """``connect_args=`` is merged into the dbapi.connect() kwargs at
    runtime, NOT at URL-parse time. Capture the actual dbapi.connect
    invocation to verify the kwarg reaches the dbapi layer.
    """
    captured: dict[str, object] = {}

    engine = create_engine(
        "dqlite://localhost:9001/test",
        connect_args={"max_message_size": 54321},
    )
    try:
        # Wrap the dialect's dbapi.connect so we can intercept the
        # actual call. SA invokes dialect.connect() which routes
        # through ``loaded_dbapi.connect(...)``; capturing here is
        # the closest observable surface to the dbapi-layer
        # propagation.
        real_connect = engine.dialect.loaded_dbapi.connect

        def _capturing_connect(*args: object, **kwargs: object) -> object:
            captured.update(kwargs)
            # Don't actually connect (no cluster needed for the
            # propagation check). Return a sentinel that SA would
            # immediately discard since we're going to dispose.
            raise RuntimeError("propagation-check stub; no real connect")

        engine.dialect.loaded_dbapi.connect = _capturing_connect  # type: ignore[attr-defined]
        try:
            with pytest.raises(RuntimeError, match="propagation-check stub"):
                engine.connect()
        finally:
            engine.dialect.loaded_dbapi.connect = real_connect  # type: ignore[attr-defined]
        assert captured.get("max_message_size") == 54321
    finally:
        engine.dispose()


def test_async_url_query_max_message_size_propagates() -> None:
    engine = create_async_engine(
        "dqlite+aio://localhost:9001/test?max_message_size=99999",
    )
    try:
        kwargs = _extract_connect_args(engine.sync_engine)
        assert kwargs.get("max_message_size") == 99999
    finally:
        # ``create_async_engine`` requires async dispose; skip cleanup
        # here since we never connected. ``engine.sync_engine.dispose``
        # is a safe sync fallback.
        engine.sync_engine.dispose()


def test_async_connect_args_max_message_size_propagates() -> None:
    """Same connect_args runtime-merge check on the async dialect."""
    captured: dict[str, object] = {}

    engine = create_async_engine(
        "dqlite+aio://localhost:9001/test",
        connect_args={"max_message_size": 77777},
    )
    try:
        sync_engine = engine.sync_engine
        real_connect = sync_engine.dialect.loaded_dbapi.connect

        def _capturing_connect(*args: object, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("propagation-check stub; no real connect")

        sync_engine.dialect.loaded_dbapi.connect = _capturing_connect  # type: ignore[attr-defined]
        try:
            with pytest.raises(RuntimeError, match="propagation-check stub"):
                sync_engine.connect()
        finally:
            sync_engine.dialect.loaded_dbapi.connect = real_connect  # type: ignore[attr-defined]
        assert captured.get("max_message_size") == 77777
    finally:
        engine.sync_engine.dispose()
