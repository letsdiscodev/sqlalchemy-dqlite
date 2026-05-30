"""is_disconnect still matches closed-handle messages with an ``(id=...)`` suffix."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import InterfaceError as DbapiInterfaceError
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "message",
    [
        "Connection is closed (id=140234123)",
        "Cursor is closed (id=140234123)",
        "cursor is closed (id=140234123)",
    ],
)
def test_is_disconnect_matches_closed_handle_with_id_suffix(message: str) -> None:
    err = DbapiInterfaceError(message)
    assert DqliteDialect().is_disconnect(err, None, None) is True, (
        f"is_disconnect classifier must still match the closed-handle "
        f"phrase even when enriched with (id=...) suffix: {message!r}"
    )
