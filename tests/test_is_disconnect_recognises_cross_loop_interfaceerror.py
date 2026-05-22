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
    """Defence: the two substrings ("closed event loop" / "different
    event loop") are dialect-specific phrases. A user-raised
    ``InterfaceError("event loop")`` alone (without the canonical
    "closed" / "different" qualifier) should NOT trip disconnect
    classification.
    """
    err = InterfaceError("event loop", code=None)
    assert dialect.is_disconnect(err, None, None) is False
