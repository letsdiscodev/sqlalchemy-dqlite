"""is_disconnect classifies the two cross-loop InterfaceError raises
("closed event loop" / "different event loop") so the pool invalidates."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect


@pytest.fixture
def dialect() -> DqliteDialect:
    return DqliteDialect()


def test_is_disconnect_recognises_bound_to_closed_event_loop_interface_error(
    dialect: DqliteDialect,
) -> None:
    """Bound-loop-GC raise: the slot is permanently dead, pool must invalidate."""
    err = InterfaceError(
        "DqliteConnection is bound to a closed event loop. "
        "Reconstruct the connection in the new loop.",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_recognises_bound_to_different_event_loop_interface_error(
    dialect: DqliteDialect,
) -> None:
    """Cross-loop-reuse raise: slot unusable in this loop, pool must invalidate."""
    err = InterfaceError(
        "DqliteConnection is bound to a different event loop. "
        "Do not share connections across event loops or OS threads.",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_does_not_false_positive_on_event_loop_substring(
    dialect: DqliteDialect,
) -> None:
    """A bare "event loop" (no "is bound to a" anchor) must not trip."""
    err = InterfaceError("event loop", code=None)
    assert dialect.is_disconnect(err, None, None) is False


def test_is_disconnect_does_not_false_positive_on_different_event_loop_topology(
    dialect: DqliteDialect,
) -> None:
    """ "different event loop" without the "is bound to a" anchor must not trip."""
    err = InterfaceError(
        "Cannot reuse a different event loop topology in this driver",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is False


def test_is_disconnect_does_not_false_positive_on_closed_event_loop_sentinel(
    dialect: DqliteDialect,
) -> None:
    """ "closed event loop" in a non-disconnect sentinel context must not trip."""
    err = InterfaceError(
        "policy: this hop requires a closed event loop sentinel",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is False


def test_is_disconnect_does_not_false_positive_on_otel_trace_message(
    dialect: DqliteDialect,
) -> None:
    """An OTel-style "different event loop" trace message must not trip."""
    err = InterfaceError("trace: different event loop seen at hop 3", code=None)
    assert dialect.is_disconnect(err, None, None) is False
