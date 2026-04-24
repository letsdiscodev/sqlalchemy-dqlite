"""``is_disconnect`` substring fallback matches case-insensitively so a
future upper-case rewording of a wire-layer / client-layer message
does not silently drop the classification.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "message",
    [
        "Connection closed by peer",
        "CONNECTION CLOSED",
        "connection CLOSED",
        "peer timed out",
        "Timed Out",
        "TIMED OUT",
        "Wire Decode Failed at offset 42",
        "wire stream error: expected ROWS, got FAILURE",
    ],
)
def test_is_disconnect_case_insensitive_substring(message: str) -> None:
    dialect = DqliteDialect()
    e = OperationalError(message)
    assert dialect.is_disconnect(e, None, None) is True


def test_unrelated_message_not_classified_as_disconnect() -> None:
    dialect = DqliteDialect()
    e = OperationalError("constraint violated")
    assert dialect.is_disconnect(e, None, None) is False
