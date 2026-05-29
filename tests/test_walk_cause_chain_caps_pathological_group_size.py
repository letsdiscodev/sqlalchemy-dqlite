"""``_walk_cause_chain`` caps distinct nodes via ``max_nodes`` to bound group fan-out work."""

from __future__ import annotations

from sqlalchemydqlite.base import _walk_cause_chain


def test_walk_cause_chain_caps_pathological_fanout() -> None:
    children = [ValueError(f"child {i}") for i in range(10_000)]
    group = BaseExceptionGroup("attack", children)
    nodes = list(_walk_cause_chain(group))
    assert len(nodes) <= 256


def test_walk_cause_chain_respects_explicit_max_nodes() -> None:
    children = [ValueError(f"child {i}") for i in range(100)]
    group = BaseExceptionGroup("test", children)
    nodes = list(_walk_cause_chain(group, max_nodes=10))
    assert len(nodes) <= 10


def test_walk_cause_chain_small_group_yields_everything() -> None:
    children = [ValueError(f"child {i}") for i in range(20)]
    group = BaseExceptionGroup("normal", children)
    nodes = list(_walk_cause_chain(group))
    assert len(nodes) == 21


def test_walk_cause_chain_max_depth_short_circuits_before_max_nodes() -> None:
    """``max_depth`` short-circuits a deep chain before the higher ``max_nodes`` cap fires."""
    head: BaseException = ValueError("base")
    for i in range(30):
        nxt = ValueError(f"link {i}")
        nxt.__cause__ = head
        head = nxt

    nodes = list(_walk_cause_chain(head, max_depth=25, max_nodes=256))
    assert len(nodes) == 25


def test_walk_cause_chain_cycle_in_cause_chain_not_inflated_in_seen() -> None:
    """A self-cycle emits once: the ``id(cur) in seen`` check sits before ``seen.add``."""
    e: BaseException = ValueError("cyclic")
    e.__cause__ = e
    nodes = list(_walk_cause_chain(e, max_nodes=10))
    assert len(nodes) == 1
    assert nodes[0] is e


def test_walk_cause_chain_cap_check_runs_at_top_of_iteration() -> None:
    """Off-by-one: ``len(seen) >= max_nodes`` is checked at the top of the loop."""
    head: BaseException = ValueError("base")
    for i in range(10):
        nxt = ValueError(f"link {i}")
        nxt.__cause__ = head
        head = nxt

    nodes = list(_walk_cause_chain(head, max_nodes=5))
    assert len(nodes) == 5
