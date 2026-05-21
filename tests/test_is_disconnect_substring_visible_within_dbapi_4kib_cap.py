"""Pin: SA's ``is_disconnect`` classifier sees disconnect substrings at
byte positions past 1024 (the client-layer cap) but within the dbapi's
4 KiB cap.

Earlier the inline comment claimed the dbapi truncates the displayed
``message`` at 1024 chars (``_MAX_DISPLAY_MESSAGE``). That is the
CLIENT layer's cap; the dbapi layer caps at
``_DEFAULT_MAX_RAW_MESSAGE`` (4 KiB). The classifier reads
``raw_message`` first so a substring past 1024 is still visible —
this test pins that path so a future "comment fix" that mistakes the
cap and drops the priority-read of ``raw_message`` is caught.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def _dialect() -> DqliteDialect:
    return DqliteDialect.__new__(DqliteDialect)


def test_disconnect_substring_at_byte_2000_classifies() -> None:
    """A disconnect substring at byte 2000 of the original message is
    within the dbapi's 4 KiB cap but past the 1024 client cap. The
    classifier sees it via the priority-read of ``raw_message``."""
    # Pad with 2000 chars, then the disconnect substring, then more
    # padding to push the substring well past the 1024 mark.
    payload = ("." * 2000) + " wire decode failed: corrupt frame " + ("." * 500)
    exc = OperationalError(payload)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True


def test_disconnect_substring_at_byte_3500_classifies() -> None:
    """Approach the upper edge of the dbapi's 4 KiB cap: the
    substring is still visible because ``raw_message`` carries the
    full server text."""
    payload = ("." * 3500) + " wire decode failed: truncated frame " + ("." * 100)
    exc = OperationalError(payload)
    dialect = _dialect()
    assert dialect.is_disconnect(exc, None, None) is True


def test_dbapi_message_cap_is_4096_not_1024() -> None:
    """Sanity: the dbapi caps the displayed ``message`` at 4 KiB, not
    1024. If a future change harmonises the caps the SA comment
    needs to be re-checked along with this assertion."""
    exc = OperationalError("y" * 10_000)
    # The truncation suffix is short; we just need to be > 1024.
    assert len(str(exc)) > 1024, (
        "dbapi OperationalError message should be capped at "
        "_DEFAULT_MAX_RAW_MESSAGE (4 KiB), not _MAX_DISPLAY_MESSAGE "
        "(1024). SA's is_disconnect comment must reflect this."
    )
