"""is_disconnect sees substrings past 1024 (client cap) but within the
dbapi's 4 KiB cap, via the priority-read of raw_message.

The 1024 cap is the client layer's; the dbapi caps at 4 KiB.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def _dialect() -> DqliteDialect:
    return DqliteDialect.__new__(DqliteDialect)


def test_disconnect_substring_at_byte_2000_classifies() -> None:
    """Substring at byte 2000: past the 1024 client cap, within 4 KiB."""
    payload = ("." * 2000) + " wire decode failed: corrupt frame " + ("." * 500)
    exc = OperationalError(payload)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True


def test_disconnect_substring_at_byte_3500_classifies() -> None:
    """Near the 4 KiB upper edge: still visible via raw_message."""
    payload = ("." * 3500) + " wire decode failed: truncated frame " + ("." * 100)
    exc = OperationalError(payload)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True


def test_dbapi_message_cap_is_4096_not_1024() -> None:
    """The dbapi caps the displayed message at 4 KiB, not 1024."""
    exc = OperationalError("y" * 10_000)
    assert len(str(exc)) > 1024, (
        "dbapi OperationalError message should be capped at "
        "_DEFAULT_MAX_RAW_MESSAGE (4 KiB), not _MAX_DISPLAY_MESSAGE "
        "(1024). SA's is_disconnect comment must reflect this."
    )
