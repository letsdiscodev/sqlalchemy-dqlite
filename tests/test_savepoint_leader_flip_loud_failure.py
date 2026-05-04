"""Pin: SAVEPOINT operations are NOT retry-safe across leader flips.

SAVEPOINT names are session-local in SQLite and cannot be
reconstructed on a post-flip connection. The dialect must surface
the failure loudly; a "helpful" reclassification of the failure as
a disconnect-equivalent would silently retry and lose the user's
nested-tx scope.

Two unit pins (below) and one integration pin cover the contract:

- Unit: ``OperationalError("no such savepoint")`` is NOT classified
  as a disconnect (no silent retry).
- Unit: when the cause-chain carries a real disconnect, that
  classification still wins (the savepoint loss is a downstream
  symptom, not the primary failure).
- Integration: trigger an actual leader flip mid-savepoint and
  observe a loud failure surface. (The exact wire error is
  ``OperationalError("not leader")`` — the connection is still
  bound to the demoted ex-leader and rejects the RELEASE — not
  ``"no such savepoint"``. The unit test's specific-wording pin is
  what guards against the silent-retry reclassification; the
  integration pin guards against silent retries on the RealError
  path that actually fires today.)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from sqlalchemydqlite import DqliteDialect

if TYPE_CHECKING:
    pass  # type-only imports go here; runtime imports are inside the test body


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


@pytest.mark.integration
def test_savepoint_release_after_leader_flip_raises_loud_failure(
    cluster_address: str,
    cluster_control: Any,
) -> None:
    """End-to-end: open a transaction, take a SAVEPOINT, force the
    cluster to flip leadership, then attempt the RELEASE — observe
    a loud failure.

    The test exercises the real production path: a single
    SQLAlchemy connection holding an open SAVEPOINT across a Raft
    leadership change. The connection's TCP is still alive but
    bound to the demoted ex-leader; the server rejects the RELEASE
    with ``OperationalError("not leader")``. The pin: the failure
    surfaces (loudly), it is NOT silently retried, and the
    connection state visible to the caller reflects the failure.

    Restores the original leader on the way out so subsequent
    tests in the session see a stable starting state.
    """
    import asyncio
    import contextlib

    from sqlalchemy import create_engine
    from sqlalchemy import text as _text
    from sqlalchemy.exc import OperationalError as SAOperationalError

    starting = asyncio.run(cluster_control.current_leader_node())
    host, port = cluster_address.split(":")
    engine = create_engine(f"dqlite://{host}:{port}/default")
    try:
        with engine.connect() as conn:
            # SQLAlchemy auto-begins the outer transaction on the
            # first execute; manually issuing SAVEPOINT keeps the
            # test surface explicit (closer to what the dialect's
            # ``do_savepoint`` emits internally) without depending on
            # the begin_nested context-manager exit timing.
            conn.execute(_text("SAVEPOINT sa_test"))

            # Force a leader flip — the connection is now bound to
            # the demoted ex-leader.
            flip = asyncio.run(cluster_control.force_leader_flip())
            assert flip.target.node_id != starting.node_id

            # The RELEASE must fail loudly. The exact wire error is
            # ``OperationalError("not leader")`` — but the contract
            # the test pins is "an error reaches the caller", not
            # the specific wording.
            with pytest.raises(SAOperationalError) as excinfo:
                conn.execute(_text("RELEASE SAVEPOINT sa_test"))
            # Sanity: the failure is from the dbapi layer (i.e. a
            # server-sourced error), not a SA-internal validation
            # error that might silently swallow the real cause.
            assert "dqlitedbapi" in str(type(excinfo.value.orig)) or hasattr(
                excinfo.value.orig, "code"
            ), (
                f"expected a dqlitedbapi-sourced OperationalError; got "
                f"{type(excinfo.value.orig).__name__}: {excinfo.value.orig}"
            )

            # Discard the broken transaction state — the connection
            # is now invalidated; SA will close it cleanly on exit.
            conn.invalidate()
    finally:
        with contextlib.suppress(Exception):
            asyncio.run(cluster_control.transfer_leadership_to(starting.node_id))
        engine.dispose()
