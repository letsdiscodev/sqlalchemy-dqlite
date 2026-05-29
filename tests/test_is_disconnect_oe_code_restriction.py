"""is_disconnect's substring scan fires only for OperationalError(code=None).

Server-routed coded errors carry user-controlled message text (e.g. RAISE) that
must NOT trip classification, else SA retries a non-idempotent INSERT.
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
    """A coded OperationalError with a transport-style substring must NOT classify."""
    # Canonical substrings must be ignored because code is set.
    e = OperationalError("user-controlled message: timed out validating peer", code=code)
    assert DqliteDialect().is_disconnect(e, None, None) is False

    e2 = OperationalError("connection closed by user trigger", code=code)
    assert DqliteDialect().is_disconnect(e2, None, None) is False
