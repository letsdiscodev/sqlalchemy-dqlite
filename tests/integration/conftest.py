"""Integration test fixtures for sqlalchemy-dqlite."""

import pytest


@pytest.fixture
def engine_url(cluster_address: str) -> str:
    host, port = cluster_address.split(":")
    return f"dqlite://{host}:{port}/test"


@pytest.fixture
def async_engine_url(cluster_address: str) -> str:
    host, port = cluster_address.split(":")
    return f"dqlite+aio://{host}:{port}/test"
