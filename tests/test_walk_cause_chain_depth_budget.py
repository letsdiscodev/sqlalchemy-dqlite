"""``_walk_cause_chain`` reaches deep leaves through group fan-out.

Group children enqueue at the parent's depth (not a wrap layer), preserving the spine-depth
budget for cause/context, and ``max_depth=25`` covers realistic wrap towers.
"""

from __future__ import annotations

import dqliteclient.exceptions as _client_exc
from sqlalchemydqlite.base import DqliteDialect, _walk_cause_chain


def _wrap(n: int, leaf: BaseException) -> BaseException:
    """Return ``n`` ``RuntimeError`` wrappers ending in ``leaf`` via ``__cause__``."""
    cur: BaseException = leaf
    for i in range(n):
        outer = RuntimeError(f"wrap-{i}")
        outer.__cause__ = cur
        cur = outer
    return cur


def test_deep_wrap_tower_with_group_at_root_classifies_leaf() -> None:
    """A group whose child is a depth-12 wrap tower to DqliteConnectionError is a disconnect."""
    leaf = _client_exc.DqliteConnectionError("addr: timed out")
    deep_wrap = _wrap(12, leaf)
    group = BaseExceptionGroup("pool init failed", [deep_wrap])
    assert DqliteDialect().is_disconnect(group, None, None) is True


def test_group_children_enqueue_at_parent_depth() -> None:
    """A group wrapping a 24-cause chain yields the leaf: children sit at the parent's depth."""
    leaf = _client_exc.DqliteConnectionError("leaf")
    deep_wrap = _wrap(24, leaf)
    group = BaseExceptionGroup("g", [deep_wrap])

    yielded = list(_walk_cause_chain(group))
    assert any(isinstance(x, _client_exc.DqliteConnectionError) for x in yielded), (
        "leaf at depth 24 must be reachable through a group at depth 0"
    )


def test_max_depth_default_raised_to_25() -> None:
    """A 24-hop pure cause chain (no groups) must reach the leaf."""
    leaf = _client_exc.DqliteConnectionError("leaf")
    chain = _wrap(24, leaf)

    yielded = list(_walk_cause_chain(chain))
    assert any(isinstance(x, _client_exc.DqliteConnectionError) for x in yielded)


def test_pathological_cycle_still_terminates() -> None:
    """Negative pin: visited-set still cycles cap at ``max_depth``."""
    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a

    yielded = list(_walk_cause_chain(a))
    assert len(yielded) == 2
    assert yielded[0] is a
    assert yielded[1] is b


def test_nested_groups_traverse_via_queue() -> None:
    """Nested groups (outer to inner to leaf) still reach the leaf."""
    leaf = _client_exc.DqliteConnectionError("leaf in nested group")
    inner_group = BaseExceptionGroup("inner", [leaf])
    outer_group = BaseExceptionGroup("outer", [inner_group])
    assert DqliteDialect().is_disconnect(outer_group, None, None) is True
