"""Pin: ``is_disconnect`` recognises the bare emitted prefix
``"event loop already running"`` from the aio adapter's remap at
``aio.py:140-141`` — independent of the appended ``str(hop)`` cause
text.

The current ``_dqlite_disconnect_messages`` set carries
``"loop is already running"`` which matches the remapped
``OperationalError`` only because ``str(hop)`` of the original
``RuntimeError("This event loop is already running")`` is appended
after the prefix. A future refactor that strips / sanitises /
redacts the appended cause text breaks classification silently —
the prefix alone (``"event loop already running:"``) does NOT
contain the substring.

Defence: pin the prefix-only match so the disconnect classification
survives wording-only refactors of the remap helper.
"""

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
    """The bare emitted prefix (no appended cause text) must
    classify as disconnect. Without this pin, a refactor that
    drops the ``str(hop)`` suffix from the remap message would
    break classification with no signal."""
    err = OperationalError("event loop already running", code=None)
    assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_recognises_event_loop_already_running_with_suffix(
    dialect: DqliteDialect,
) -> None:
    """Existing shape (prefix + appended cause text) must still
    classify — this is the live shape the aio.py remap emits today
    and it MUST continue to match."""
    err = OperationalError(
        "event loop already running: This event loop is already running",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is True
