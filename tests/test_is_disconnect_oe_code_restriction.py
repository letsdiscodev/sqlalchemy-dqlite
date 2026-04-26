"""Pin: ``is_disconnect`` substring scan only fires for
``OperationalError(code=None)``.

The wire-decode / ProtocolError / cross-loop-remap surface produces
``OperationalError(message, code=None)`` — the substring scan is the
SOLE classifier on that path. Server-routed coded errors carry
user-controlled message text (e.g. ``RAISE(FAIL, '...timed out
validating peer ...')`` → SQLITE_ERROR code=1) that must NOT trip
disconnect classification — otherwise SA invalidates the slot and may
retry a non-idempotent INSERT against a healthy connection.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def test_operational_error_code_none_substring_classifies_disconnect() -> None:
    e = OperationalError("Wire decode failed: corrupt frame", code=None)
    assert DqliteDialect().is_disconnect(e, None, None) is True


@pytest.mark.parametrize("code", [1, 5, 19, 21])
def test_operational_error_with_code_substring_does_not_classify(code: int) -> None:
    """A server-routed coded OperationalError whose message happens to
    contain a transport-style substring (typically via user
    ``RAISE(...)`` text) must NOT classify as disconnect."""
    # Use a message that includes the canonical substrings — they must
    # be ignored because ``code`` is set.
    e = OperationalError("user-controlled message: timed out validating peer", code=code)
    assert DqliteDialect().is_disconnect(e, None, None) is False

    e2 = OperationalError("connection closed by user trigger", code=code)
    assert DqliteDialect().is_disconnect(e2, None, None) is False
