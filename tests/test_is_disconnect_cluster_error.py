"""is_disconnect recognises ClusterError via the type-dispatch walk.

ClusterPolicyError (a ClusterError subclass) is excluded: policy rejections are
deterministic config errors, so classifying them as disconnect would retry a
permanent rejection.
"""

from __future__ import annotations

import pytest

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class TestClusterErrorDisconnect:
    def test_direct_cluster_error_matches(self) -> None:
        dialect = DqliteDialect()
        assert dialect.is_disconnect(_client_exc.ClusterError("no leader"), None, None) is True

    def test_wrapped_cluster_error_matches_via_cause(self) -> None:
        dialect = DqliteDialect()
        inner = _client_exc.ClusterError("Could not find leader. Errors: ...")
        try:
            raise _dbapi_exc.OperationalError(str(inner), code=None) from inner
        except _dbapi_exc.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True


class TestClusterPolicyErrorNotDisconnect:
    def test_direct_policy_error_not_disconnect(self) -> None:
        dialect = DqliteDialect()
        assert dialect.is_disconnect(_client_exc.ClusterPolicyError("policy"), None, None) is False

    def test_wrapped_policy_error_not_disconnect(self) -> None:
        dialect = DqliteDialect()
        policy = _client_exc.ClusterPolicyError("policy rejected")
        try:
            raise _dbapi_exc.ProgrammingError(
                f"Cluster policy rejected leader: {policy}"
            ) from policy
        except _dbapi_exc.ProgrammingError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is False

    def test_policy_error_under_cluster_error_wrap_short_circuits(self) -> None:
        """Policy branch must win over a plain ClusterError, else the pool retries forever."""
        dialect = DqliteDialect()
        policy = _client_exc.ClusterPolicyError("policy rejected")
        # Walk sees the policy first (the outer exception), so it short-circuits to False.
        outer = _dbapi_exc.ProgrammingError("wrap")
        outer.__cause__ = policy
        assert dialect.is_disconnect(outer, None, None) is False


class TestClusterPolicyInterfaceErrorWrap:
    """A ClusterPolicyError wrapped as InterfaceError must NOT match the narrow
    "connection/cursor is closed" set, so the pool won't retry."""

    def test_cluster_policy_interface_error_not_disconnect(self) -> None:
        dialect = DqliteDialect()
        policy = _client_exc.ClusterPolicyError("leader not in allow-list")
        try:
            raise _dbapi_exc.InterfaceError(f"Cluster policy rejection; {policy}") from policy
        except _dbapi_exc.InterfaceError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is False


@pytest.mark.parametrize(
    "wrapped_exc_kind",
    [
        "operational",
        "interface",
        "programming",
    ],
)
def test_all_wrap_kinds_walk_to_cluster_error(wrapped_exc_kind: str) -> None:
    """The inner ClusterError surfaces via the chain walk for any dbapi wrap class."""
    dialect = DqliteDialect()
    inner = _client_exc.ClusterError("cluster down")
    if wrapped_exc_kind == "operational":
        outer: _dbapi_exc.Error = _dbapi_exc.OperationalError("wrap", code=None)
    elif wrapped_exc_kind == "interface":
        outer = _dbapi_exc.InterfaceError("wrap")
    else:
        outer = _dbapi_exc.ProgrammingError("wrap")
    outer.__cause__ = inner
    assert dialect.is_disconnect(outer, None, None) is True
