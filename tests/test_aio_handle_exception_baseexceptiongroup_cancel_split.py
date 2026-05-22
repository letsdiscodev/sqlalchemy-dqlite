"""Pin: ``AsyncAdaptedConnection._handle_exception`` splits out
``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit`` children
from a ``BaseExceptionGroup`` and re-raises them rather than
re-raising the raw group.

Without the split, SA's downstream ``isinstance(e, dbapi.Error)``
gate misses the group entirely — the pool slot stays live and the
raw group propagates to the user (a non-PEP-249 surface). Mirrors
the dbapi-layer ``_call_client`` cancel-class discipline so the
discipline is consistent end-to-end (cursor.py +
connection.py × 2 + SA aio.py).
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    """Construct a minimal AsyncAdaptedConnection for _handle_exception
    testing. The hook does not consult ``self._connection`` so we can
    drive it on a synthetic adapter.
    """
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    return adapter


def test_handle_exception_cancel_only_group_propagates_as_group() -> None:
    """The cancel-only group propagates as a group AND ``from None``
    sets ``__suppress_context__ = True`` so SA's display layer
    elides the implicit context chain in tracebacks.

    Note: under Python's auto-context machinery, ``from None`` only
    affects ``__suppress_context__`` (display) — ``__context__`` is
    still set to whatever exception was active at the raise site.
    Production callers invoke ``_handle_exception`` from inside
    ``except BaseException as error: self._handle_exception(error)``,
    so ``__context__`` is populated. ``from None`` correctly
    suppresses its display via ``__suppress_context__``. See the
    production-shape test below for the load-bearing assertion.
    """
    adapter = _make_adapter()
    eg = BaseExceptionGroup("cancel-only", [asyncio.CancelledError()])
    # Drive the helper from inside an active ``except`` so Python's
    # auto-context machinery has a candidate to attach — mirrors the
    # production call shape from ``AsyncAdaptedConnection`` flows.
    try:
        raise eg
    except BaseExceptionGroup as caught_eg:
        with pytest.raises(BaseExceptionGroup) as excinfo:
            adapter._handle_exception(caught_eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)
    # ``raise cancel_group from None`` sets __suppress_context__ for
    # display, NOT __context__ — the latter is populated by Python's
    # auto-context machinery because the call site was inside an
    # active ``except`` block (mirroring the production shape).
    # __cause__ IS cleared by ``from None``.
    assert excinfo.value.__suppress_context__ is True, (
        "``from None`` must set __suppress_context__ = True so SA's "
        "traceback layer elides the implicit context chain"
    )
    assert excinfo.value.__cause__ is None, "``from None`` must clear __cause__"


def test_handle_exception_production_shape_preserves_context_link() -> None:
    """Load-bearing production-shape pin: when invoked from inside
    an active ``except`` block (as the live ``AsyncAdaptedConnection``
    callers do), the propagated cancel group's ``__context__``
    points back to the caught group for forensic traceback walkers.
    The display layer suppresses it via ``__suppress_context__``,
    but the link is preserved.
    """
    adapter = _make_adapter()
    eg = BaseExceptionGroup("cancel-only", [asyncio.CancelledError("c")])
    try:
        raise eg
    except BaseExceptionGroup as caught_eg:
        with pytest.raises(BaseExceptionGroup) as excinfo:
            adapter._handle_exception(caught_eg)
        assert excinfo.value.__context__ is caught_eg, (
            "Python's auto-context machinery must link the propagated "
            "group to the caught group; only display is suppressed"
        )
        assert excinfo.value.__suppress_context__ is True


def test_handle_exception_mixed_group_with_loop_state_child_still_propagates_cancel() -> None:
    """Precedence: a group containing BOTH a CancelledError AND a
    loop-state RuntimeError (the canonical mixed-shape from a
    cross-loop ``TaskGroup``) must propagate the CancelledError
    rather than fire ``_remap_loop_state_runtime_error`` on the
    loop-state hop.

    Without the cancel-class split running first, the remap walks
    the group, finds the loop-state child, raises
    ``OperationalError`` from that hop, and the cancel-class child
    is silently dropped — exactly the failure mode the cancel-class
    split was added to prevent.
    """
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "mixed-with-loop-state",
        [asyncio.CancelledError(), RuntimeError("different event loop")],
    )
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)
    # Group must contain the cancel partition only; the loop-state
    # RuntimeError remains accessible via the original group on
    # __context__ for diagnostic walks if the consumer chooses.


def test_handle_exception_loop_state_only_group_still_remaps_to_operational_error() -> None:
    """Negative pin: a group with NO cancel-class child but a
    loop-state RuntimeError must still surface as
    ``OperationalError`` via ``_remap_loop_state_runtime_error``.
    The reorder must not regress the original remap behaviour on
    pure loop-state groups.
    """
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "pure-loop-state",
        [RuntimeError("different event loop")],
    )
    with pytest.raises(OperationalError):
        adapter._handle_exception(eg)


def test_handle_exception_mixed_group_propagates_cancel_partition() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "mixed",
        [asyncio.CancelledError(), OperationalError("oops", code=1)],
    )
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)


def test_handle_exception_pure_exception_group_wraps_as_operationalerror() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "transport-multi",
        [OperationalError("a", code=1), OperationalError("b", code=1)],
    )
    with pytest.raises(OperationalError) as excinfo:
        adapter._handle_exception(eg)
    # Remainder is accessible on __cause__ for SA's walk_cause_chain.
    assert isinstance(excinfo.value.__cause__, BaseExceptionGroup)


def test_handle_exception_keyboard_interrupt_group_propagates() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup("ki", [KeyboardInterrupt()])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, KeyboardInterrupt) for c in inner)


def test_handle_exception_systemexit_group_propagates() -> None:
    adapter = _make_adapter()
    eg = BaseExceptionGroup("se", [SystemExit(1)])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, SystemExit) for c in inner)


def test_handle_exception_non_group_passes_through_unchanged() -> None:
    """Non-BaseExceptionGroup errors take the existing ``raise error``
    path — the split is added without disturbing the prior behaviour.
    """
    adapter = _make_adapter()
    oe = OperationalError("plain", code=1)
    with pytest.raises(OperationalError) as excinfo:
        adapter._handle_exception(oe)
    assert excinfo.value is oe
