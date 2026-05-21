"""Pin: ``_remap_loop_state_runtime_error`` docstring documents the
close-arm's bespoke ``"event loop is closed"`` divergence.

The helper raises on each of three substring matches; the close-arm
of ``AsyncAdaptedConnection.close`` routes ``"different loop"`` and
``"loop is already running"`` through the helper (via
``_handle_exception``) but handles ``"event loop is closed"`` with a
bespoke ``logger.debug + return`` because the ``has_terminate=True``
dialect promise says ``close()`` must not propagate. Without a note
on the helper itself, a future contributor "consolidating" the
close-arm by routing all three substrings through the helper would
compile cleanly while silently breaking the must-not-raise
invariant. The docstring NOTE is the contract surface; this pin
keeps it from regressing.
"""

from __future__ import annotations

from sqlalchemydqlite.aio import _remap_loop_state_runtime_error


def test_helper_docstring_notes_close_arm_bespoke_divergence() -> None:
    text = _remap_loop_state_runtime_error.__doc__ or ""
    lowered = text.lower()
    # The NOTE must name the close arm AND the must-not-raise reason.
    assert "close" in lowered, "Helper docstring should name the close-arm's bespoke handling."
    assert "must-not-raise" in lowered or "must not raise" in lowered or "has_terminate" in text, (
        "Helper docstring should name the must-not-raise / has_terminate "
        "invariant the close-arm's bespoke handling preserves."
    )
    assert "event loop is closed" in text, (
        "Helper docstring should name the specific substring the close-arm handles bespoke."
    )
