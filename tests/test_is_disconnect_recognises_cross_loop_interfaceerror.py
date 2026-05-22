"""Pin: SA dialect's ``is_disconnect`` classifies the two cross-loop
``InterfaceError`` raise-sites at
``dqliteclient/connection.py:2207`` (closed event loop) and
``:2212`` (different event loop) as disconnects so the pool
invalidates the slot.

Previously the InterfaceError arm of ``is_disconnect`` only matched
``"connection is closed"`` / ``"cursor is closed"`` /
``"connection invalidated (id="`` / ``"used after fork"``. The two
cross-loop substrings ("closed event loop" / "different event
loop") were absent: the bound-loop-GC and cross-loop-reuse signals
slipped past, keeping a dead slot in the pool until the next
retry — at which point the same InterfaceError fires again with
the same classification gap.
"""

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
    """The client's bound-loop-GC raise at
    ``dqliteclient/connection.py:2207`` surfaces as
    ``InterfaceError("DqliteConnection is bound to a closed event
    loop. ...")`` after dbapi translation. The slot is permanently
    dead (the bound loop was GC'd); pool MUST invalidate.
    """
    err = InterfaceError(
        "DqliteConnection is bound to a closed event loop. "
        "Reconstruct the connection in the new loop.",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_recognises_bound_to_different_event_loop_interface_error(
    dialect: DqliteDialect,
) -> None:
    """The client's cross-loop-reuse raise at
    ``dqliteclient/connection.py:2212`` surfaces as
    ``InterfaceError("DqliteConnection is bound to a different
    event loop. ...")``. The slot is unusable in the current loop;
    pool MUST invalidate.
    """
    err = InterfaceError(
        "DqliteConnection is bound to a different event loop. "
        "Do not share connections across event loops or OS threads.",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is True


def test_is_disconnect_does_not_false_positive_on_event_loop_substring(
    dialect: DqliteDialect,
) -> None:
    """Defence: the canonical raise-site phrases carry ``"is bound to a"``.
    A user-raised ``InterfaceError("event loop")`` alone (no
    qualifier) should NOT trip disconnect classification.
    """
    err = InterfaceError("event loop", code=None)
    assert dialect.is_disconnect(err, None, None) is False


def test_is_disconnect_does_not_false_positive_on_different_event_loop_topology(
    dialect: DqliteDialect,
) -> None:
    """Defence: a user-raised ``InterfaceError`` mentioning
    ``"different event loop"`` in a non-disconnect context
    (e.g. ``"Cannot reuse a different event loop topology in this
    driver"``) must NOT trip. The tightened ``"is bound to a"``
    anchor prevents false-positive slot invalidation.
    """
    err = InterfaceError(
        "Cannot reuse a different event loop topology in this driver",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is False


def test_is_disconnect_does_not_false_positive_on_closed_event_loop_sentinel(
    dialect: DqliteDialect,
) -> None:
    """Defence: an operator-policy InterfaceError mentioning
    ``"closed event loop"`` in a non-disconnect sentinel context
    must NOT trip.
    """
    err = InterfaceError(
        "policy: this hop requires a closed event loop sentinel",
        code=None,
    )
    assert dialect.is_disconnect(err, None, None) is False


def test_is_disconnect_does_not_false_positive_on_otel_trace_message(
    dialect: DqliteDialect,
) -> None:
    """Defence: an OTel-style trace message mentioning
    ``"different event loop"`` must NOT trip. The canonical raise
    sites carry the ``"is bound to a"`` qualifier which this
    user-wording does not.
    """
    err = InterfaceError("trace: different event loop seen at hop 3", code=None)
    assert dialect.is_disconnect(err, None, None) is False
