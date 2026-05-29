"""``_walk_cause_chain`` terminates on a cycle that loops back near the depth cap."""

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
    seen_ids = {id(x) for x in yielded}
    assert len(seen_ids) == len(yielded)
    assert any(isinstance(x, _client_exc.DqliteConnectionError) for x in yielded)
