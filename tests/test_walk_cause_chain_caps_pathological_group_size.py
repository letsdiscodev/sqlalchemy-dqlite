"""Pin: ``_walk_cause_chain`` caps total distinct nodes via ``max_nodes``
so a pathological ``BaseExceptionGroup`` with thousands of unique
children does not drag every ``is_disconnect`` call into O(N) work in
the group size.
"""

from __future__ import annotations

from sqlalchemydqlite.base import _walk_cause_chain


def test_walk_cause_chain_caps_pathological_fanout() -> None:
    children = [ValueError(f"child {i}") for i in range(10_000)]
    group = BaseExceptionGroup("attack", children)
    nodes = list(_walk_cause_chain(group))
    # Default cap is 256; we should see at most that many before the
    # walker bails. The exact count depends on traversal order but is
    # bounded by the cap.
    assert len(nodes) <= 256


def test_walk_cause_chain_respects_explicit_max_nodes() -> None:
    children = [ValueError(f"child {i}") for i in range(100)]
    group = BaseExceptionGroup("test", children)
    nodes = list(_walk_cause_chain(group, max_nodes=10))
    assert len(nodes) <= 10


def test_walk_cause_chain_small_group_yields_everything() -> None:
    """A realistic-sized group is fully traversed (the cap doesn't
    kick in for normal retry / telemetry / circuit-breaker towers).
    """
    children = [ValueError(f"child {i}") for i in range(20)]
    group = BaseExceptionGroup("normal", children)
    nodes = list(_walk_cause_chain(group))
    # 1 group + 20 children = 21 nodes; well below the 256 cap.
    assert len(nodes) == 21
