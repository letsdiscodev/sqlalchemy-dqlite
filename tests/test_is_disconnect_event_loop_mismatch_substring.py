"""The canonical ``"event-loop mismatch:"`` prefix is the durable disconnect signal.

The legacy ``"different loop"`` / ``"different event loop"`` substrings were
redundant and could false-positive on benign user message text, so only the
prefix classifies.
"""

from __future__ import annotations

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


def test_event_loop_mismatch_remapped_form_classified_as_disconnect() -> None:
    """The canonical remap form is matched."""
    dialect = DqliteDialect()
    e = _dbapi_exc.OperationalError(
        "event-loop mismatch: <Future ... attached to a different loop>",
        code=None,
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_event_loop_mismatch_remapped_dbapi_wording_classified() -> None:
    """The dbapi-side wording is also prefixed by the remap and matches."""
    dialect = DqliteDialect()
    e = _dbapi_exc.OperationalError(
        "event-loop mismatch: AsyncConnection ... called from a different event loop",
        code=None,
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_user_message_with_different_loop_no_prefix_not_classified() -> None:
    """A code=None OperationalError with the legacy "different loop" text but no
    canonical prefix MUST NOT invalidate the slot."""
    dialect = DqliteDialect()
    e = _dbapi_exc.OperationalError(
        "user-trigger said 'different loop' but everything is fine",
        code=None,
    )
    assert dialect.is_disconnect(e, None, None) is False


def test_user_message_with_different_event_loop_no_prefix_not_classified() -> None:
    """Symmetric pin for the dbapi-side legacy substring."""
    dialect = DqliteDialect()
    e = _dbapi_exc.OperationalError(
        "user-trigger said 'different event loop' but everything is fine",
        code=None,
    )
    assert dialect.is_disconnect(e, None, None) is False
