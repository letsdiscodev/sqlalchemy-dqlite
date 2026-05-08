"""Pytest configuration for sqlalchemy-dqlite tests."""

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _clear_resolve_leader_cache() -> Iterator[None]:
    """Clear the process-wide ``_RESOLVE_LEADER_CACHE`` between tests.

    SA tests reach into ``dqlitedbapi.connect`` (transitively via
    ``create_engine``) which populates the same module-global cache
    that the dbapi-side autouse fixture already clears
    (``python-dqlite-dbapi/tests/conftest.py:10-22``). Without an
    equivalent fixture here, two SA tests that target the same
    ``(address, governors)`` tuple via different ``ClusterClient``
    mocks would share a stale cached instance from the earlier test's
    patch context — the same flake vector the dbapi fixture was added
    to prevent.
    """
    from dqlitedbapi import connection as _conn_mod

    _conn_mod._RESOLVE_LEADER_CACHE.clear()
    yield
    _conn_mod._RESOLVE_LEADER_CACHE.clear()


# Add python-dqlite-dev's testlib to sys.path so tests (in particular
# the leader-flip integration suite) can import shared utilities from
# ``dqlitetestlib``. ``python-dqlite-dev`` is expected as a sibling of
# this checkout — see ``python-dqlite-dev/testlib/README.md``. The
# insertion is harmless when the sibling repo is absent (path won't
# exist; nothing imports the testlib from unit tests anyway).
_TESTLIB = Path(__file__).resolve().parent.parent.parent / "python-dqlite-dev" / "testlib"
if _TESTLIB.exists() and str(_TESTLIB) not in sys.path:
    sys.path.insert(0, str(_TESTLIB))

# Pytest 8+ requires ``pytest_plugins`` at the top-level conftest.
# Only register the testlib's fixtures plugin when the path resolved
# so consumers running unit tests without the sibling repo see no
# difference.
if _TESTLIB.exists():
    pytest_plugins = ["dqlitetestlib.fixtures"]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: marks tests as requiring dqlite cluster")


@pytest.fixture
def cluster_address() -> str:
    """Get the test cluster address."""
    return os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
