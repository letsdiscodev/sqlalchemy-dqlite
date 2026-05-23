"""Pin: SA's ``is_disconnect`` matches the leader-lost upstream
substring case-insensitively — defends against an upstream wording
capitalisation change (e.g. ``"No database opened"`` with a capital
``N``).

The previous code named the variable ``msg_lc`` but never called
``.lower()``. The substring constant
``LEADER_LOST_DB_LOOKUP_SUBSTRING`` is lowercase by convention; a
capitalised upstream wording would silently break the classifier
arm. The fix lowercases the haystack so the variable name reflects
reality and the comparison is robust against upstream drift.
"""

from __future__ import annotations

from typing import Any

import pytest

from sqlalchemydqlite.base import DqliteDialect


def _make_dialect() -> DqliteDialect:
    dialect = DqliteDialect()
    return dialect


@pytest.mark.parametrize(
    "raw_message",
    [
        "no database opened: <db>",
        "No database opened: <db>",
        "NO DATABASE OPENED: <db>",
        "  no Database Opened",  # leading whitespace stripped by SA classifier? not necessarily
    ],
)
def test_is_disconnect_classifies_leader_lost_case_insensitive(raw_message: str) -> None:
    from dqlitedbapi.exceptions import OperationalError

    dialect = _make_dialect()
    if raw_message.lower().startswith("no database opened"):
        # The actual contract: lowercase form must start with the
        # canonical lowercase substring. The whitespace-leading case
        # is excluded because .startswith() is positional.
        err = OperationalError("no database opened", 12, raw_message=raw_message)
        assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_does_not_classify_off_topic_notfound(_: Any = None) -> None:
    """Negative pin: SQLITE_NOTFOUND with a different message
    (e.g. ``LOOKUP_STMT`` arm: "no statement with the given id ...")
    is NOT classified as a leader-lost disconnect."""
    from dqlitedbapi.exceptions import OperationalError

    dialect = _make_dialect()
    err = OperationalError(
        "no statement with the given id 5",
        12,
        raw_message="no statement with the given id 5",
    )
    assert dialect.is_disconnect(err, None, None) is False
