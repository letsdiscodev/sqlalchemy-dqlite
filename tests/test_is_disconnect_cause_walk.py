"""is_disconnect walks the full __cause__/__context__ chain so the inner transport
cause is found no matter how many wrap layers (retry/telemetry/circuit-breaker) are added."""

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
        """__context__ (set by a bare raise in an except block) must be walked too,
        else non-`from` wraps drop the disconnect signal."""
        dialect = DqliteDialect()
        try:
            try:
                raise _client_exc.DqliteConnectionError("peer rst")
            except _client_exc.DqliteConnectionError:
                # No `from e`; __context__ captures the inner.
                raise _dbapi_exc.OperationalError("implicit chain")  # noqa: B904
        except _dbapi_exc.OperationalError as outer:
            assert dialect.is_disconnect(outer, None, None) is True

    def test_self_cycle_does_not_hang(self) -> None:
        """A pathological ``raise X from X`` self-cycle must not spin."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("loop")
        e.__cause__ = e
        assert dialect.is_disconnect(e, None, None) is False


class TestWalkBoundedDepth:
    def test_very_deep_chain_does_not_hang(self) -> None:
        """A deep non-cyclic chain must terminate promptly."""
        dialect = DqliteDialect()
        deep: BaseException = _dbapi_exc.OperationalError("leaf")
        for i in range(50):
            wrap = _dbapi_exc.OperationalError(f"wrap {i}")
            wrap.__cause__ = deep
            deep = wrap
        _ = dialect.is_disconnect(deep, None, None)

    def test_disconnect_within_cap_is_found(self) -> None:
        """A disconnect at depth 9 (inside the 25-hop cap) must still classify."""
        dialect = DqliteDialect()
        target = _client_exc.DqliteConnectionError("peer rst")
        cur: BaseException = target
        for i in range(8):
            wrap = _dbapi_exc.OperationalError(f"wrap {i}")
            wrap.__cause__ = cur
            cur = wrap
        top = _dbapi_exc.OperationalError("top")
        top.__cause__ = cur  # target is at depth 9

        assert dialect.is_disconnect(top, None, None) is True

    def test_disconnect_beyond_cap_is_not_found_by_type_walk(self) -> None:
        """A disconnect past the 25-hop cutoff is invisible to the type walk; the outer
        message is non-matching so the substring fallback can't flip the verdict."""
        dialect = DqliteDialect()
        target = _client_exc.DqliteConnectionError("peer rst")
        cur: BaseException = target
        for i in range(25):
            wrap = _dbapi_exc.OperationalError(f"wrap {i}")
            wrap.__cause__ = cur
            cur = wrap
        top = _dbapi_exc.OperationalError("nothing matching here")
        top.__cause__ = cur  # target is at depth 26 past the cap

        assert dialect.is_disconnect(top, None, None) is False
