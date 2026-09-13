"""Pytest configuration for sqlalchemy-dqlite tests."""

import os
import sys
from pathlib import Path

import pytest

# The sibling python-dqlite-dev checkout provides dqlitetestlib (cluster fixtures).
_TESTLIB = Path(__file__).resolve().parent.parent.parent / "python-dqlite-dev" / "testlib"
if _TESTLIB.exists() and str(_TESTLIB) not in sys.path:
    sys.path.insert(0, str(_TESTLIB))
if _TESTLIB.exists():
    pytest_plugins = ["dqlitetestlib.fixtures"]


@pytest.fixture
def cluster_address() -> str:
    return os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
