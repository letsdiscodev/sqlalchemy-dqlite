"""Pin the canonical event-loop-mismatch substring on is_disconnect.

Every cross-loop RuntimeError that reaches ``is_disconnect``'s
``code=None`` ``OperationalError`` substring scan was first remapped
by ``aio._handle_exception`` to:

    OperationalError(f"event-loop mismatch: {original_msg}", code=None)

The legacy substrings ``"different loop"`` and ``"different event
loop"`` were left over from an earlier version that scanned the bare
``RuntimeError``/``ProgrammingError`` text directly. They were both
redundant — any match they could see today is also covered by the
canonical ``"event-loop mismatch:"`` prefix — and both could
false-positive against a benign user-supplied error message in a
``code=None`` ``OperationalError`` (e.g. a custom retry decorator
that logs a context message containing those words).

Pin both directions:

- the canonical remapped form is classified as disconnect;
- a benign code=None OperationalError that happens to mention
  "different loop" / "different event loop" without the canonical
  prefix is NOT classified as disconnect.
"""

from __future__ import annotations

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import DqliteDialect


def test_event_loop_mismatch_remapped_form_classified_as_disconnect() -> None:
    """The canonical ``aio._handle_exception`` remap form is matched.
    Note the original substring still appears inside the prefix-led
    remap; both shapes converge to the prefix."""
    dialect = DqliteDialect()
    e = _dbapi_exc.OperationalError(
        "event-loop mismatch: <Future ... attached to a different loop>",
        code=None,
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_event_loop_mismatch_remapped_dbapi_wording_classified() -> None:
    """The dbapi-side wording (``"different event loop"``) is also
    routed through the remap and prefixed; the same prefix substring
    matches it."""
    dialect = DqliteDialect()
    e = _dbapi_exc.OperationalError(
        "event-loop mismatch: AsyncConnection ... called from a different event loop",
        code=None,
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_user_message_with_different_loop_no_prefix_not_classified() -> None:
    """A user-supplied OperationalError(code=None) whose text happens
    to contain the legacy substring ``"different loop"`` (without the
    canonical ``"event-loop mismatch:"`` prefix) MUST NOT trigger a
    pool-invalidation. The legacy substring was load-bearing only
    because the remap prefixed every match; the prefix itself is the
    durable signal."""
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
