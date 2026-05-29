"""Upstream-provenance pin: ``_RAFT_COLLAPSE_DISCONNECT_MARKERS`` phrases are
owned by raft's ``err.h`` (not a formal API) — an upstream rename would
silently disable SA's classifier. Skipped when upstream isn't in the workspace."""

from __future__ import annotations

import pathlib

import pytest

from sqlalchemydqlite.base import _RAFT_COLLAPSE_DISCONNECT_MARKERS


def _upstream_err_h() -> pathlib.Path | None:
    """Locate dqlite-upstream's ``raft/err.h`` by walking up to the workspace root."""
    here = pathlib.Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / "dqlite-upstream" / "src" / "raft" / "err.h"
        if candidate.is_file():
            return candidate
    return None


def test_raft_marker_phrases_appear_in_upstream_err_h() -> None:
    err_h = _upstream_err_h()
    if err_h is None:
        pytest.skip("dqlite-upstream/src/raft/err.h not in workspace")
    source = err_h.read_text()
    for marker in _RAFT_COLLAPSE_DISCONNECT_MARKERS:
        assert marker in source, (
            f"SA marker {marker!r} no longer appears in {err_h}. "
            f"Either upstream raft renamed the phrase (update SA's marker "
            f"set in lockstep) or the X-macro entry was removed (drop the "
            f"marker)."
        )
