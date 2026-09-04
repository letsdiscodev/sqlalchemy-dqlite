"""Pytest configuration for sqlalchemy-dqlite tests."""

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _reset_unparseable_iso_warning_gate() -> Iterator[None]:
    """Reset each type-class's one-shot ``_unparseable_iso_warning_emitted``
    ClassVar per test; it would otherwise persist and silence WARNINGs that
    later tests want to observe.
    """
    from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime

    for cls in (_DqliteDateTime, _DqliteDate, _DqliteTime):
        cls._unparseable_iso_warning_emitted = False
    yield
    for cls in (_DqliteDateTime, _DqliteDate, _DqliteTime):
        cls._unparseable_iso_warning_emitted = False


@pytest.fixture(autouse=True)
def _restore_adapters() -> Iterator[None]:
    """Snapshot and restore the ``_ADAPTERS`` registry between tests so a test
    that registers a custom adapter can't leak it into the next."""
    from dqlitedbapi.types import _ADAPTERS

    snapshot = dict(_ADAPTERS)
    try:
        yield
    finally:
        _ADAPTERS.clear()
        _ADAPTERS.update(snapshot)


# Add the sibling python-dqlite-dev testlib to sys.path so integration tests
# can import ``dqlitetestlib``; harmless when the sibling repo is absent.
_TESTLIB = Path(__file__).resolve().parent.parent.parent / "python-dqlite-dev" / "testlib"
if _TESTLIB.exists() and str(_TESTLIB) not in sys.path:
    sys.path.insert(0, str(_TESTLIB))

# Pytest 8+ requires ``pytest_plugins`` at the top-level conftest.
if _TESTLIB.exists():
    pytest_plugins = ["dqlitetestlib.fixtures"]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: marks tests as requiring dqlite cluster")


@pytest.fixture
def cluster_address() -> str:
    return os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
