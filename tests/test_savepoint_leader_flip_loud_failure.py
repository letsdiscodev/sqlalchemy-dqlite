"""Pin: SAVEPOINT operations are NOT retry-safe across leader flips.

If a leader flip arrives between ``do_savepoint`` (emitting
``SAVEPOINT sa_savepoint_N``) and the subsequent ``release_savepoint``,
the new leader has no record of the savepoint. The reset path emits
``RELEASE SAVEPOINT sa_savepoint_N`` against a connection that knows
nothing about it, surfacing as ``OperationalError("no such savepoint:
sa_savepoint_N")``.

This is INTENTIONALLY a loud failure: SAVEPOINT names are session-local
in SQLite and cannot be reconstructed on the new leader. Pin the loud
behaviour so a future "helpful" reclassification of "no such savepoint"
as a disconnect-equivalent (which would silently retry) can't ship
without an obvious test update.
"""

from __future__ import annotations

import pytest

from sqlalchemydqlite import DqliteDialect


def test_no_such_savepoint_is_not_classified_as_disconnect() -> None:
    """``OperationalError("no such savepoint")`` must NOT be classified
    as a disconnect by ``is_disconnect``. Disconnect classification
    would route the SA pool slot through the reconnect-and-retry path
    on RELEASE SAVEPOINT — but the lost-savepoint error is a logical
    inconsistency, not a transport failure, and silently retrying
    would mask the loss of the user's nested-tx scope.
    """
    import dqlitedbapi.exceptions

    dialect = DqliteDialect()
    e = dqlitedbapi.exceptions.OperationalError("no such savepoint: sa_savepoint_1")
    assert dialect.is_disconnect(e, None, None) is False


def test_no_such_savepoint_with_chained_disconnect_cause_is_disconnect() -> None:
    """If the underlying cause IS a disconnect (DqliteConnectionError
    / leader-change OperationalError), the chain-walk classification
    should still surface that as a disconnect — the savepoint loss is
    a downstream symptom, not the primary failure."""
    import dqliteclient.exceptions
    import dqlitedbapi.exceptions

    dialect = DqliteDialect()
    transport_failure = dqliteclient.exceptions.DqliteConnectionError("peer RST")
    try:
        raise dqlitedbapi.exceptions.OperationalError(
            "no such savepoint: sa_savepoint_1"
        ) from transport_failure
    except dqlitedbapi.exceptions.OperationalError as wrapped:
        # Walk-cause-chain catches the transport failure.
        assert dialect.is_disconnect(wrapped, None, None) is True


@pytest.mark.skip(
    reason=(
        "Requires a multi-node fixture with controlled leader demotion. "
        "Track separately under integration infrastructure work — for "
        "now the unit pin above documents the loud-failure contract."
    )
)
def test_savepoint_release_after_leader_flip_raises_no_such_savepoint() -> None:
    """Integration sketch — needs a leader-demotion fixture to run."""
