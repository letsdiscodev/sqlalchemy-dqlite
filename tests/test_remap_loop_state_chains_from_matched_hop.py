"""Pin: ``_remap_loop_state_runtime_error`` chains the new
``OperationalError`` from the *matched* ``hop`` (the loop-state
``RuntimeError`` / ``ProgrammingError`` that gave the diagnostic its
name), not from the *outer* ``error`` scope.

Background
----------

The helper walks ``_walk_cause_chain(error)`` looking for a ``hop``
whose text matches one of three loop-state substrings, then re-raises
``OperationalError(...) from <X>``. The discipline across the rest of
the codebase (``cursor.py::_call_client``, the connect-time arms in
``connection.py``) chains ``from`` the *matched* exception, so the
discriminator sits at ``__cause__`` depth 1. Chaining from the outer
``error`` instead would bury the discriminator behind the wrap layers
and degrade diagnostics for bounded-depth chain walkers (SIEM /
log-forwarding tooling that defensively caps at depth 1-2).

PEP 3134 §"The ``from`` clause": ``raise X from Y`` sets
``__cause__ = Y`` AND ``__suppress_context__ = True``. The IMMEDIATE
cause of the new exception is exactly what follows ``from``.

This is a diagnostic-fidelity pin, not a functional one — SA's
``walk_cause_chain`` descends to the discriminator either way via the
full chain walk, so ``is_disconnect`` classification is unaffected.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import OperationalError, ProgrammingError
from sqlalchemydqlite.aio import _remap_loop_state_runtime_error


def test_remap_chains_from_matched_hop_not_outer_error_runtime() -> None:
    """A 3-deep cause chain ending in a loop-state ``RuntimeError``
    must surface the matched hop at ``__cause__`` depth 1."""
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
    """The helper also catches ``ProgrammingError`` shapes from the
    dbapi's cross-loop reuse path; the same discriminator-at-depth-1
    discipline applies."""
    hop = ProgrammingError("AsyncConnection used from a different event loop")
    error = OperationalError("outer wrap")
    error.__cause__ = hop

    with pytest.raises(OperationalError, match="event-loop mismatch") as excinfo:
        _remap_loop_state_runtime_error(error)

    assert excinfo.value.__cause__ is hop


def test_remap_bare_runtime_error_cause_is_self() -> None:
    """Regression pin: when ``error`` is itself the matched hop (no
    wrap layers), ``__cause__`` is the same ``error`` instance —
    ``hop is error`` in the single-element walk."""
    err = RuntimeError("Event loop is closed")

    with pytest.raises(OperationalError) as excinfo:
        _remap_loop_state_runtime_error(err)

    assert excinfo.value.__cause__ is err
