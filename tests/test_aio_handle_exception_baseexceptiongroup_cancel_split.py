"""``_handle_exception`` splits CancelledError / KeyboardInterrupt /
SystemExit children out of a BaseExceptionGroup and re-raises them;
otherwise SA's ``isinstance(e, dbapi.Error)`` gate misses the group
and the pool slot stays live."""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    return adapter


def test_handle_exception_cancel_only_group_propagates_as_group() -> None:
    """Cancel-only group propagates as a group with
    ``__suppress_context__ = True`` (from ``raise ... from None``)."""
    adapter = _make_adapter()
    eg = BaseExceptionGroup("cancel-only", [asyncio.CancelledError()])
    # Drive from inside an active ``except`` so auto-context has a
    # candidate to attach, mirroring the production call shape.
    try:
        raise eg
    except BaseExceptionGroup as caught_eg:
        with pytest.raises(BaseExceptionGroup) as excinfo:
            adapter._handle_exception(caught_eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)
    assert excinfo.value.__suppress_context__ is True, (
        "``from None`` must set __suppress_context__ = True so SA's "
        "traceback layer elides the implicit context chain"
    )
    assert excinfo.value.__cause__ is None, "``from None`` must clear __cause__"


def test_raise_from_none_sets_suppress_context_on_non_split_group() -> None:
    """Isolate ``from None`` semantics on a handcrafted group that never
    passed through ``split()`` (which would itself set the flag), so this
    fires if a maintainer drops ``from None`` from the production raise."""
    eg = BaseExceptionGroup("handcrafted", [asyncio.CancelledError("c")])
    assert eg.__suppress_context__ is False
    try:
        raise RuntimeError("outer")
    except RuntimeError:
        try:
            raise eg from None
        except BaseExceptionGroup as got:
            assert got.__suppress_context__ is True, (
                "``raise eg from None`` MUST set __suppress_context__ = True. "
                "Without this assertion, a maintainer who drops `from None` "
                "would only be caught by the __cause__ pin."
            )


def test_handle_exception_production_shape_preserves_context_link() -> None:
    """Invoked from inside an active ``except`` (as live callers do), the
    propagated group's ``__context__`` still links back to the caught
    group; only display is suppressed."""
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
    """Cancel-class split runs first: a group with both a CancelledError
    and a loop-state RuntimeError must propagate the CancelledError, not
    remap to OperationalError and drop the cancel child."""
    adapter = _make_adapter()
    eg = BaseExceptionGroup(
        "mixed-with-loop-state",
        [asyncio.CancelledError(), RuntimeError("different event loop")],
    )
    with pytest.raises(BaseExceptionGroup) as excinfo:
        adapter._handle_exception(eg)
    inner = excinfo.value.exceptions
    assert any(isinstance(c, asyncio.CancelledError) for c in inner)


def test_handle_exception_loop_state_only_group_still_remaps_to_operational_error() -> None:
    """A group with no cancel child but a loop-state RuntimeError must
    still surface as OperationalError; the reorder must not regress it."""
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
    # Remainder is on __cause__ for SA's walk_cause_chain.
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
    """Non-group errors take the existing ``raise error`` path
    unchanged."""
    adapter = _make_adapter()
    oe = OperationalError("plain", code=1)
    with pytest.raises(OperationalError) as excinfo:
        adapter._handle_exception(oe)
    assert excinfo.value is oe
