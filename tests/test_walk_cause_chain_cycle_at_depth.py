"""Pin: ``_walk_cause_chain`` terminates on a cycle that loops back
near the depth cap.

Existing tests cover small (depth 1, 2-cycle) cycles + 24-deep
no-cycle. Not covered: 24-deep then cycle back to root. The
visited-set should catch it before the depth cap fires; this test
exercises that ordering.
"""

from __future__ import annotations

import dqliteclient.exceptions as _client_exc
from sqlalchemydqlite.base import _walk_cause_chain


def test_deep_chain_with_back_cycle_terminates_via_visited_set() -> None:
    leaf = _client_exc.DqliteConnectionError("leaf")
    cur: BaseException = leaf
    chain_nodes: list[BaseException] = [leaf]
    for i in range(23):
        outer = RuntimeError(f"wrap-{i}")
        outer.__cause__ = cur
        chain_nodes.append(outer)
        cur = outer
    # Cycle the leaf back to the chain root (depth 23).
    leaf.__cause__ = cur

    yielded = list(_walk_cause_chain(cur))
    # Each unique exception object appears at most once.
    seen_ids = {id(x) for x in yielded}
    assert len(seen_ids) == len(yielded)
    # The leaf is reachable.
    assert any(isinstance(x, _client_exc.DqliteConnectionError) for x in yielded)
