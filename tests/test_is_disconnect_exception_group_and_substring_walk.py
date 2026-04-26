"""Pin: is_disconnect unwraps BaseExceptionGroup AND walks the cause chain
for the substring fallback.

The pool's ``initialize`` aggregates multiple connect failures via
``raise BaseExceptionGroup("...", [DqliteConnectionError(...), ...])``.
Without unwrapping the group's children, the SA dialect's
``is_disconnect`` would never see the wrapped causes and the group
would propagate as a non-disconnect error — the pool would not
recover from a multi-failure connect cascade.

The substring fallback was previously single-hop on the exception
type, so a user-side retry decorator wrapping the dbapi
``OperationalError("Wire decode failed: ...")`` would defeat
disconnect classification entirely (wire-decode errors carry
``code=None`` so the leader-code branch can't catch them).
"""

from __future__ import annotations

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


def _dialect() -> DqliteDialect:
    return DqliteDialect()


class TestExceptionGroupUnwrap:
    def test_dqlite_connection_error_inside_group_classified_as_disconnect(self) -> None:
        inner1 = _client_exc.DqliteConnectionError("addr1: timed out")
        inner2 = _client_exc.DqliteConnectionError("addr2: connection refused")
        group = BaseExceptionGroup("pool init failed", [inner1, inner2])
        assert _dialect().is_disconnect(group, None, None) is True

    def test_nested_exception_group(self) -> None:
        inner = _client_exc.DqliteConnectionError("addr: timed out")
        inner_group = BaseExceptionGroup("inner", [inner])
        outer = BaseExceptionGroup("outer", [inner_group])
        assert _dialect().is_disconnect(outer, None, None) is True

    def test_unrelated_exception_group_not_classified(self) -> None:
        group = BaseExceptionGroup("unrelated", [ValueError("x"), ValueError("y")])
        assert _dialect().is_disconnect(group, None, None) is False

    def test_dqlite_connection_error_via_cause_then_group(self) -> None:
        leaf = _client_exc.DqliteConnectionError("addr: timed out")
        wrapper = RuntimeError("wrap")
        wrapper.__cause__ = leaf
        group = BaseExceptionGroup("g", [wrapper])
        assert _dialect().is_disconnect(group, None, None) is True


class TestSubstringBranchCauseWalk:
    def test_wire_decode_failed_via_cause_walk(self) -> None:
        """An OperationalError(message, code=None) from a wire-decode
        failure wrapped in another exception must still classify as
        disconnect via the substring fallback's cause walk."""
        inner = _dbapi_exc.OperationalError("Wire decode failed: corrupt frame")

        class _MyAppError(Exception):
            pass

        outer = _MyAppError("application wrapper")
        outer.__cause__ = inner
        assert _dialect().is_disconnect(outer, None, None) is True

    def test_connection_closed_via_context_walk(self) -> None:
        inner = _dbapi_exc.OperationalError("connection closed")
        try:
            try:
                raise inner
            except Exception:
                raise RuntimeError("ctx") from None
        except RuntimeError as outer:
            assert _dialect().is_disconnect(outer, None, None) is True

    def test_substring_branch_still_catches_direct_exception(self) -> None:
        """Negative pin: the cause-walk widening must not lose the
        original direct-isinstance behaviour."""
        exc = _dbapi_exc.OperationalError("Wire decode failed: bad header")
        assert _dialect().is_disconnect(exc, None, None) is True
