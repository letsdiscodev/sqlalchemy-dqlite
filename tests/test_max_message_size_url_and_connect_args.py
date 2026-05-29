"""Pin: ``max_message_size`` flows from both the SA URL query and
``connect_args=`` into the dbapi layer."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import create_async_engine


def _extract_connect_args(engine: object) -> dict[str, object]:
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
    """``?max_message_size=none`` (use wire default) must propagate as ``None``."""
    engine = create_engine(
        "dqlite://localhost:9001/test?max_message_size=none",
    )
    try:
        kwargs = _extract_connect_args(engine)
        assert kwargs.get("max_message_size") is None
    finally:
        engine.dispose()


def test_url_query_max_message_size_negative_rejected() -> None:
    """Negative values rejected at URL-parse time, not deferred to first checkout."""
    with pytest.raises(ArgumentError):
        create_engine("dqlite://localhost:9001/test?max_message_size=-1")


def test_url_query_max_message_size_non_int_rejected() -> None:
    with pytest.raises(ArgumentError):
        create_engine("dqlite://localhost:9001/test?max_message_size=abc")


def test_connect_args_max_message_size_propagates() -> None:
    """``connect_args=`` merges into dbapi.connect() kwargs at runtime, not URL-parse."""
    captured: dict[str, object] = {}

    engine = create_engine(
        "dqlite://localhost:9001/test",
        connect_args={"max_message_size": 54321},
    )
    try:
        real_connect = engine.dialect.loaded_dbapi.connect

        def _capturing_connect(*args: object, **kwargs: object) -> object:
            captured.update(kwargs)
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
        # Never connected, so sync dispose of the inner engine is a safe fallback.
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
