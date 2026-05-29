"""is_disconnect classifies RAFT errors the C layer collapses to code=1."""

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
            # Case-insensitive: raft_strerror is lowercase but pin both.
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
            # Marker set is whole canonical phrases; substrings must not match.
            "the system is shutting down soon",
            "user said operation cancelled (different spelling)",
            "no connection found",
            "i/o error in user trigger",  # excluded marker
            "out of memory in user trigger",  # excluded marker
        ],
    )
    def test_benign_code1_messages_not_classified(self, benign_message: str) -> None:
        """code=1 with a non-marker message must not classify as disconnect."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError(benign_message, code=1)
        assert dialect.is_disconnect(e, None, None) is False

    def test_code_none_substring_scan_still_runs(self) -> None:
        """The existing ``code is None`` substring scan is unaffected."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("event-loop mismatch: ...", code=None)
        assert dialect.is_disconnect(e, None, None) is True

    def test_code_other_than_1_with_marker_text_not_classified(self) -> None:
        """Widening is gated on code == 1; other coded errors with marker
        text carry user-controlled messages and must not classify."""
        dialect = DqliteDialect()
        e = _dbapi_exc.OperationalError("server is shutting down", code=5)  # BUSY
        assert dialect.is_disconnect(e, None, None) is False
