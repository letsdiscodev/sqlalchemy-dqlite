"""Cross-package upstream-provenance pin for SA's raft-collapse markers.

The SA dialect's ``_RAFT_COLLAPSE_DISCONNECT_MARKERS`` carries three
phrases the dqlite-server's ``translateRaftErrCode`` default arm
emits via ``raft_strerror`` for ``RAFT_SHUTDOWN`` /
``RAFT_CANCELED`` / ``RAFT_NOCONNECTION``. The phrasing is owned by
the upstream raft library (``raft/err.h``) and is not part of any
formal API — a raft library upgrade renaming "server is shutting
down" to "server going down" silently disables SA's classifier on
this narrow ``code=1`` channel.

If the upstream source is present in this workspace (as
``/home/antoine/src/dqlite/dqlite-upstream/``) this test reads
``raft/err.h`` and asserts the SA marker phrases still appear in
the upstream X-macro table. The test is skipped when upstream is
absent (CI on a developer machine without the upstream checkout).
"""

from __future__ import annotations

import pathlib

import pytest

from sqlalchemydqlite.base import _RAFT_COLLAPSE_DISCONNECT_MARKERS


def _upstream_err_h() -> pathlib.Path | None:
    """Locate dqlite-upstream's ``raft/err.h`` if available.

    The canonical workspace layout puts it at
    ``<workspace>/dqlite-upstream/src/raft/err.h`` sibling-to the
    four Python packages. Walks up from this file's directory to
    find the workspace root.
    """
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
