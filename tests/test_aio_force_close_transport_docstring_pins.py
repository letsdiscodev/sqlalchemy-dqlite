"""``_force_close_transport`` absorbs Exception / CancelledError but
propagates KeyboardInterrupt / SystemExit; its docstring must not
claim "Never raises"."""

from __future__ import annotations

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


class _RaisesKI:
    def force_close_transport(self) -> None:
        raise KeyboardInterrupt


class _RaisesSE:
    def force_close_transport(self) -> None:
        raise SystemExit(1)


def _make_adapter(inner: object) -> AsyncAdaptedConnection:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = inner
    return adapter


def test_force_close_transport_propagates_keyboard_interrupt() -> None:
    """KeyboardInterrupt must propagate so signal-driven dispose()
    cannot mask the interrupt."""
    adapter = _make_adapter(_RaisesKI())
    with pytest.raises(KeyboardInterrupt):
        adapter._force_close_transport()


def test_force_close_transport_propagates_system_exit() -> None:
    """SystemExit propagates so interpreter shutdown sees the exit
    request unmodified."""
    adapter = _make_adapter(_RaisesSE())
    with pytest.raises(SystemExit):
        adapter._force_close_transport()


def test_force_close_transport_docstring_does_not_promise_never_raises() -> None:
    """The docstring must not recur to "Never raises" — it overpromises
    vs the actual catch breadth."""
    text = AsyncAdaptedConnection._force_close_transport.__doc__ or ""
    assert "Never raises" not in text, (
        "Docstring overpromises: KeyboardInterrupt / SystemExit are "
        "intentionally NOT caught by the except clause."
    )
