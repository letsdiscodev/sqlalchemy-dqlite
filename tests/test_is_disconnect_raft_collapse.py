"""Pin: ``DqliteDialect.is_disconnect`` classifies cluster-mgmt
RAFT errors that the C ``translateRaftErrCode`` default arm collapses
to ``OperationalError(code=1)``.

The C ``translateRaftErrCode`` (``src/translate.c`` in dqlite-upstream)
collapses every non-NOTLEADER / non-LEADERSHIPLOST / non-CANTCHANGE
raft error to ``SQLITE_ERROR=1`` with the verbatim ``raft_strerror``
text attached. Without this pin, the dialect's standard ``code is
None`` gate disables the substring scan, and these errors do NOT
classify as disconnect — leaving SA's pool to keep a torn-state
slot.

The pin asserts the narrow second channel: ``code=1`` + a
tightly-bounded marker in ``_RAFT_COLLAPSE_DISCONNECT_MARKERS`` →
disconnect. Negative pins assert user-controlled message text
without the canonical marker does NOT false-positive.
"""

from __future__ import annotations

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


class TestIsDisconnectRaftCollapse:
    @pytest.mark.parametrize(
        "message",
        [
            "server is shutting down",
            "operation canceled",
            "no connection to remote server available",
            # Case-insensitive (raft_strerror is lowercase but pin both):
            "Server is shutting down",
            "OPERATION CANCELED",
        ],
    )
    def test_raft_collapse_markers_classify_as_disconnect(self, message: str) -> None:
        """code=1 + a canonical RAFT marker → disconnect."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError(message, code=1)
        assert dialect.is_disconnect(e, None, None) is True

    @pytest.mark.parametrize(
        "benign_message",
        [
            # Substring of a marker is NOT in the marker set — user
            # trigger messages like these MUST NOT false-positive.
            "the system is shutting down soon",
            "user said operation cancelled (different spelling)",
            "no connection found",
            "i/o error in user trigger",  # excluded marker
            "out of memory in user trigger",  # excluded marker
        ],
    )
    def test_benign_code1_messages_not_classified(self, benign_message: str) -> None:
        """code=1 with a non-marker message must not classify as
        disconnect. The marker set is whole canonical phrases, not
        single words."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError(benign_message, code=1)
        assert dialect.is_disconnect(e, None, None) is False

    def test_code_none_substring_scan_still_runs(self) -> None:
        """Sanity: the existing ``code is None`` substring scan is
        unaffected. ``"event-loop mismatch:"`` is in
        ``_dqlite_disconnect_messages``."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("event-loop mismatch: ...", code=None)
        assert dialect.is_disconnect(e, None, None) is True

    def test_code_other_than_1_with_marker_text_not_classified(self) -> None:
        """The widening is gated on ``code == 1`` specifically; other
        coded OperationalErrors with marker text in the message must
        not classify (server-routed coded errors carry user-controlled
        message text). Pin against silent widening."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("server is shutting down", code=5)  # BUSY
        assert dialect.is_disconnect(e, None, None) is False
