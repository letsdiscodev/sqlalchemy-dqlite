"""is_disconnect recognises ClusterError via the type-dispatch walk.

The dbapi's ``_call_client`` wraps non-policy ``ClusterError`` into a
bare ``OperationalError`` with no SQLite code — the substring list
cannot match its typical message shape ("Could not find leader.
Errors: [...]"), so a leader-blip mid-query used to surface as a
non-disconnect and the pool kept the broken slot.

``ClusterPolicyError`` (a ``ClusterError`` subclass) is excluded from
the disconnect set: policy rejections are deterministic configuration
errors and classifying them as disconnect would re-enter the retry
loop against a permanent rejection (see the ISSUE-600 rationale in
``_call_client``).
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
        """If a chain surfaces both a ``ClusterPolicyError`` and a
        plain ``ClusterError`` as causes, the policy branch must win
        — otherwise the pool would retry a permanent-reject config.
        """
        dialect = DqliteDialect()
        policy = _client_exc.ClusterPolicyError("policy rejected")
        # Construct a chain policy -> plain cluster error via
        # ``__cause__``. The walk sees the policy first (top of
        # chain is the outer exception the user caught), so it
        # short-circuits to False.
        outer = _dbapi_exc.ProgrammingError("wrap")
        outer.__cause__ = policy
        assert dialect.is_disconnect(outer, None, None) is False


@pytest.mark.parametrize(
    "wrapped_exc_kind",
    [
        "operational",
        "interface",
        "programming",
    ],
)
def test_all_wrap_kinds_walk_to_cluster_error(wrapped_exc_kind: str) -> None:
    """Regardless of which dbapi wrap class is chosen, the inner
    ``ClusterError`` surfaces via the chain walk."""
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
