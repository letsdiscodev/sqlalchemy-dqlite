"""Pytest configuration for sqlalchemy-dqlite tests."""

import os

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: marks tests as requiring dqlite cluster")


@pytest.fixture
def cluster_address() -> str:
    """Get the test cluster address."""
    return os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
