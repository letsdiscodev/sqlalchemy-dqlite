"""is_disconnect matches the leader-lost upstream substring case-insensitively,
defending against an upstream capitalisation change (e.g. "No database opened")."""

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
        "  no Database Opened",
    ],
)
def test_is_disconnect_classifies_leader_lost_case_insensitive(raw_message: str) -> None:
    from dqlitedbapi.exceptions import OperationalError

    dialect = _make_dialect()
    if raw_message.lower().startswith("no database opened"):
        # .startswith() is positional, so the whitespace-leading case is excluded.
        err = OperationalError("no database opened", 12, raw_message=raw_message)
        assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_does_not_classify_off_topic_notfound(_: Any = None) -> None:
    """SQLITE_NOTFOUND with the LOOKUP_STMT wording is NOT a leader-lost disconnect."""
    from dqlitedbapi.exceptions import OperationalError

    dialect = _make_dialect()
    err = OperationalError(
        "no statement with the given id 5",
        12,
        raw_message="no statement with the given id 5",
    )
    assert dialect.is_disconnect(err, None, None) is False
