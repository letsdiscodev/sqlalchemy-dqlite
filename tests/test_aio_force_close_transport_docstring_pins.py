"""Pin: ``AsyncAdaptedConnection._force_close_transport`` docstring
does not promise "Never raises", and the behavioural contract holds —
``KeyboardInterrupt`` / ``SystemExit`` (and any other non-``Exception``
``BaseException``) propagate while every ``Exception`` /
``CancelledError`` from the hook call is absorbed.

The earlier docstring claimed "Never raises", which conflicted with
the actual ``except (Exception, asyncio.CancelledError):`` catch and
misled IDE-hover readers writing surrounding code that expected the
method to be safe from signal-handler-driven shutdown contexts.
"""

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
    """KeyboardInterrupt is a BaseException that is NOT a subclass
    of Exception; the ``except (Exception, asyncio.CancelledError):``
    catch deliberately does not absorb it so a signal-handler-driven
    ``engine.dispose()`` cannot mask the interrupt."""
    adapter = _make_adapter(_RaisesKI())
    with pytest.raises(KeyboardInterrupt):
        adapter._force_close_transport()


def test_force_close_transport_propagates_system_exit() -> None:
    """Symmetric: SystemExit propagates so interpreter shutdown
    paths see the exit request unmodified."""
    adapter = _make_adapter(_RaisesSE())
    with pytest.raises(SystemExit):
        adapter._force_close_transport()


def test_force_close_transport_docstring_does_not_promise_never_raises() -> None:
    """Pin against the literal "Never raises" wording recurring —
    that wording overpromises vs the actual catch breadth."""
    text = AsyncAdaptedConnection._force_close_transport.__doc__ or ""
    assert "Never raises" not in text, (
        "Docstring overpromises: KeyboardInterrupt / SystemExit are "
        "intentionally NOT caught by the except clause."
    )
