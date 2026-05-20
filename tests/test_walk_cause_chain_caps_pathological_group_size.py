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


def test_walk_cause_chain_max_depth_short_circuits_before_max_nodes() -> None:
    """A deeply-nested ``__cause__`` chain emits at most
    ``max_depth=25`` nodes even though ``max_nodes=256`` is well
    above the chain length. Documents the depth-vs-node-cap
    interaction so a regression that drops the ``depth >= max_depth``
    arm — leaving the walker to traverse arbitrarily-deep chains
    until the node cap fires — would fail with a clear diagnostic."""
    head: BaseException = ValueError("base")
    for i in range(30):
        nxt = ValueError(f"link {i}")
        nxt.__cause__ = head
        head = nxt

    nodes = list(_walk_cause_chain(head, max_depth=25, max_nodes=256))
    # ``depth=0`` is the head; ``depth=25`` is the first node skipped
    # via the ``depth >= max_depth`` arm. Total emitted: 25.
    assert len(nodes) == 25


def test_walk_cause_chain_cycle_in_cause_chain_not_inflated_in_seen() -> None:
    """A self-referential ``__cause__`` cycle emits the node exactly
    once and the ``len(seen)`` counter sees it as 1 — the cap doesn't
    fire spuriously. Pins the ``id(cur) in seen`` check sitting BEFORE
    ``seen.add(id(cur))``."""
    e: BaseException = ValueError("cyclic")
    e.__cause__ = e
    nodes = list(_walk_cause_chain(e, max_nodes=10))
    assert len(nodes) == 1
    assert nodes[0] is e


def test_walk_cause_chain_cap_check_runs_at_top_of_iteration() -> None:
    """Off-by-one pin: ``len(seen) >= max_nodes`` is checked at the
    TOP of the while loop, so ``max_nodes=5`` against a 10-deep
    chain emits exactly 5 nodes (not 6). A regression that moves the
    check to the bottom would silently over-emit by 1."""
    head: BaseException = ValueError("base")
    for i in range(10):
        nxt = ValueError(f"link {i}")
        nxt.__cause__ = head
        head = nxt

    nodes = list(_walk_cause_chain(head, max_nodes=5))
    assert len(nodes) == 5
