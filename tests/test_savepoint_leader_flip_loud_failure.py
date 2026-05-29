"""SAVEPOINT operations are NOT retry-safe across leader flips: names are
session-local and unreconstructable post-flip, so the dialect must surface the
failure loudly rather than reclassify it as a disconnect and silently retry."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from sqlalchemydqlite import DqliteDialect

if TYPE_CHECKING:
    pass


def test_no_such_savepoint_is_not_classified_as_disconnect() -> None:
    """A lost savepoint is a logical inconsistency, not a transport failure;
    classifying it as a disconnect would silently retry and mask the loss."""
    import dqlitedbapi.exceptions

    dialect = DqliteDialect()
    e = dqlitedbapi.exceptions.OperationalError("no such savepoint: sa_savepoint_1")
    assert dialect.is_disconnect(e, None, None) is False


def test_no_such_savepoint_with_chained_disconnect_cause_is_disconnect() -> None:
    """When the cause-chain carries a real disconnect, that classification
    wins; the savepoint loss is a downstream symptom, not the primary failure."""
    import dqliteclient.exceptions
    import dqlitedbapi.exceptions

    dialect = DqliteDialect()
    transport_failure = dqliteclient.exceptions.DqliteConnectionError("peer RST")
    try:
        raise dqlitedbapi.exceptions.OperationalError(
            "no such savepoint: sa_savepoint_1"
        ) from transport_failure
    except dqlitedbapi.exceptions.OperationalError as wrapped:
        assert dialect.is_disconnect(wrapped, None, None) is True


@pytest.mark.integration
def test_savepoint_release_after_leader_flip_raises_loud_failure(
    cluster_address: str,
    cluster_control: Any,
) -> None:
    """End-to-end: SAVEPOINT, force a leader flip, then RELEASE surfaces a loud
    failure (not silently retried). Restores the original leader on exit."""
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
            # Manual SAVEPOINT keeps the surface explicit, avoiding
            # begin_nested context-manager exit-timing dependence.
            conn.execute(_text("SAVEPOINT sa_test"))

            flip = asyncio.run(cluster_control.force_leader_flip())
            assert flip.target.node_id != starting.node_id

            # The contract is "an error reaches the caller", not the
            # specific wording (the wire error is "not leader").
            with pytest.raises(SAOperationalError) as excinfo:
                conn.execute(_text("RELEASE SAVEPOINT sa_test"))
            # Sanity: the failure is server-sourced (dbapi layer), not a
            # SA-internal validation error swallowing the real cause.
            assert "dqlitedbapi" in str(type(excinfo.value.orig)) or hasattr(
                excinfo.value.orig, "code"
            ), (
                f"expected a dqlitedbapi-sourced OperationalError; got "
                f"{type(excinfo.value.orig).__name__}: {excinfo.value.orig}"
            )

            conn.invalidate()
    finally:
        with contextlib.suppress(Exception):
            asyncio.run(cluster_control.transfer_leadership_to(starting.node_id))
        engine.dispose()
