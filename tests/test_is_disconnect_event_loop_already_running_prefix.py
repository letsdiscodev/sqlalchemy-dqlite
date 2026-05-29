"""is_disconnect recognises the bare ``"event loop already running"`` prefix from
the aio remap, independent of the appended ``str(hop)`` cause text — so dropping
that suffix in a refactor still classifies."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


@pytest.fixture
def dialect() -> DqliteDialect:
    return DqliteDialect()


def test_is_disconnect_recognises_event_loop_already_running_prefix(
    dialect: DqliteDialect,
) -> None:
    """The bare prefix (no appended cause text) must classify as disconnect."""
    err = OperationalError("event loop already running", code=None)
    assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_recognises_event_loop_already_running_with_suffix(
    dialect: DqliteDialect,
) -> None:
    """The live shape (prefix + appended cause text) must still classify."""
    err = OperationalError(
        "event loop already running: This event loop is already running",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is True
