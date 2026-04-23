"""is_disconnect walks the full ``__cause__`` / ``__context__`` chain.

A single-hop check catches today's ``_call_client`` wrap but breaks as
soon as any further wrap layer is added (retry decorator, telemetry
middleware, circuit breaker). The bounded-walk variant surfaces the
inner transport-level cause no matter how deep the wrap tower is.
"""

from __future__ import annotations

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class TestCauseChainWalk:
    def test_direct_connection_error_still_matches(self) -> None:
        dialect = DqliteDialect()
        assert dialect.is_disconnect(_client_exc.DqliteConnectionError("rst"), None, None) is True

    def test_one_hop_cause_still_matches(self) -> None:
        dialect = DqliteDialect()
        original = _client_exc.DqliteConnectionError("peer rst")
        try:
            raise _dbapi_exc.OperationalError("wrapped") from original
        except _dbapi_exc.OperationalError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_two_hop_cause_matches(self) -> None:
        """A retry middleware, telemetry shim, or circuit breaker can
        add a second wrap layer; the walk must descend through it."""
        dialect = DqliteDialect()
        inner = _client_exc.DqliteConnectionError("peer rst")
        try:
            try:
                raise inner
            except _client_exc.DqliteConnectionError as e:
                raise _dbapi_exc.OperationalError("wrap 1") from e
        except _dbapi_exc.OperationalError as wrap1:
            try:
                raise _dbapi_exc.OperationalError("wrap 2") from wrap1
            except _dbapi_exc.OperationalError as outer:
                assert dialect.is_disconnect(outer, None, None) is True

    def test_context_fallback_when_cause_is_none(self) -> None:
        """``__context__`` is set implicitly by a ``raise`` inside an
        ``except`` block even without ``from e``. The walk must
        traverse it too — otherwise non-``from`` wraps silently drop
        the disconnect signal.
        """
        dialect = DqliteDialect()
        try:
            try:
                raise _client_exc.DqliteConnectionError("peer rst")
            except _client_exc.DqliteConnectionError:
                # Note: no ``from e``; __context__ captures the inner.
                raise _dbapi_exc.OperationalError("implicit chain")  # noqa: B904
        except _dbapi_exc.OperationalError as outer:
            assert dialect.is_disconnect(outer, None, None) is True

    def test_self_cycle_does_not_hang(self) -> None:
        """A pathological ``raise X from X`` must not spin."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("loop")
        e.__cause__ = e  # self-cycle
        # No assertion on True/False — the invariant is "returns
        # promptly without recursing / looping." Substring-branch
        # should reach False.
        assert dialect.is_disconnect(e, None, None) is False


class TestWalkBoundedDepth:
    def test_very_deep_chain_does_not_hang(self) -> None:
        """Build a deep non-cyclic chain and verify the walk terminates."""
        dialect = DqliteDialect()
        deep: BaseException = _dbapi_exc.OperationalError("leaf")
        for i in range(50):
            wrap = _dbapi_exc.OperationalError(f"wrap {i}")
            wrap.__cause__ = deep
            deep = wrap
        # Must return promptly; no assertion on True/False — the leaf
        # is a bare OperationalError, so substring-branch handles the
        # final decision.
        _ = dialect.is_disconnect(deep, None, None)
