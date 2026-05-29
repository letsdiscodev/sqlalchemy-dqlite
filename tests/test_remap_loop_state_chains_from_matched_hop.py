"""Pin: ``_remap_loop_state_runtime_error`` chains the new ``OperationalError``
from the *matched* hop (so the discriminator sits at ``__cause__`` depth 1 for
bounded-depth chain walkers), not from the outer ``error``. Diagnostic fidelity
only — ``is_disconnect`` is unaffected."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError, ProgrammingError
from sqlalchemydqlite.aio import _remap_loop_state_runtime_error


def test_remap_chains_from_matched_hop_not_outer_error_runtime() -> None:
    """A 3-deep chain ending in a loop-state ``RuntimeError`` surfaces the
    matched hop at ``__cause__`` depth 1."""
    hop = RuntimeError("event loop is closed")
    middle = OperationalError("dbapi wrap")
    middle.__cause__ = hop
    error = OperationalError("AsyncAdapt close failure")
    error.__cause__ = middle

    with pytest.raises(OperationalError) as excinfo:
        _remap_loop_state_runtime_error(error)

    assert excinfo.value.__cause__ is hop, (
        f"discriminator not on __cause__ at depth 1; got {type(excinfo.value.__cause__).__name__}"
    )


def test_remap_chains_from_matched_hop_different_loop() -> None:
    """Same pin for the ``different loop`` arm."""
    hop = RuntimeError("Task attached to a different loop")
    middle = OperationalError("inner")
    middle.__cause__ = hop
    error = OperationalError("outer")
    error.__cause__ = middle

    with pytest.raises(OperationalError, match="event-loop mismatch") as excinfo:
        _remap_loop_state_runtime_error(error)

    assert excinfo.value.__cause__ is hop


def test_remap_chains_from_matched_hop_loop_already_running() -> None:
    """Same pin for the ``loop is already running`` arm."""
    hop = RuntimeError("This event loop is already running")
    middle = OperationalError("inner")
    middle.__cause__ = hop
    error = OperationalError("outer")
    error.__cause__ = middle

    with pytest.raises(OperationalError, match="event loop already running") as excinfo:
        _remap_loop_state_runtime_error(error)

    assert excinfo.value.__cause__ is hop


def test_remap_chains_from_matched_hop_programming_error() -> None:
    """``ProgrammingError`` shapes from the cross-loop reuse path get the same
    discriminator-at-depth-1 treatment."""
    hop = ProgrammingError("AsyncConnection used from a different event loop")
    error = OperationalError("outer wrap")
    error.__cause__ = hop

    with pytest.raises(OperationalError, match="event-loop mismatch") as excinfo:
        _remap_loop_state_runtime_error(error)

    assert excinfo.value.__cause__ is hop


def test_remap_bare_runtime_error_cause_is_self() -> None:
    """When ``error`` is itself the matched hop, ``__cause__`` is that instance."""
    err = RuntimeError("Event loop is closed")

    with pytest.raises(OperationalError) as excinfo:
        _remap_loop_state_runtime_error(err)

    assert excinfo.value.__cause__ is err
