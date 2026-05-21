"""Pin: ``do_close``'s transport-class fallback reaches
``force_close_transport`` (sync teardown) on BOTH sync and async
dialects. This is intentional — by the time the fallback fires,
the async close has already been tried and failed; reaching back
through async close machinery just to fail again would be
over-engineering. ``aio.py``'s ``force_close_transport`` public
alias is the documented sync-teardown surface for exactly this
leg.

Documentation pin so a future contributor doesn't override
``do_close`` on the async dialect to "use async-native teardown
on the fallback path" — that would duplicate effort with no
behavioural gain on a leg the async close has already failed.
"""

from __future__ import annotations

import inspect

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_do_close_docstring_documents_intentional_sync_fallback() -> None:
    """The sync ``do_close`` docstring must explain why the fallback
    is sync on both dialects."""
    src = inspect.getsource(DqliteDialect.do_close)
    assert "INTENTIONALLY" in src
    assert "_force_close_transport" in src
    assert "fallback" in src.lower()


def test_async_dialect_inherits_do_close_intentionally() -> None:
    """The async dialect does NOT override ``do_close`` because the
    inherited shape is correct. A future override would force the
    contributor to justify duplicating SA's pipeline on a
    failure-leg-only path."""
    assert "do_close" not in DqliteDialect_aio.__dict__, (
        "DqliteDialect_aio inherits do_close from the sync dialect; "
        "the inherited fallback reaches force_close_transport (sync) "
        "which is the documented sync-teardown surface — see the "
        "sync do_close docstring for rationale."
    )


def test_do_close_body_does_not_bounce_through_await_only_or_greenlet() -> None:
    """AST/source pin: the fallback must NOT route through
    ``await_only`` / ``greenlet_spawn`` / ``asyncio.to_thread``. The
    whole point of "sync teardown on async dialect" is that the
    fallback runs synchronously on the SA-pool thread. A regression
    that wraps ``force_close_transport`` in ``await_only(asyncio.to_thread(...))``
    would still trip ``force_close_transport.assert_called_once()`` but
    bounce through the loop — a contract break invisible to the
    behavioural sibling test."""
    src = inspect.getsource(DqliteDialect.do_close)
    assert "await_only" not in src, (
        "DqliteDialect.do_close (inherited by async dialect) must NOT "
        "use await_only on the fallback leg — sync teardown is "
        "intentional on both dialects."
    )
    assert "greenlet_spawn" not in src
    assert "to_thread" not in src
    # And the async sibling really is the same function object — the
    # async dialect inherits, not overrides.
    assert DqliteDialect_aio.do_close is DqliteDialect.do_close


def test_async_do_close_fallback_runs_in_calling_thread_not_loop() -> None:
    """Behavioural pin: capture the thread on which
    ``force_close_transport`` runs. The fallback must run on the
    SA-pool-thread (the caller), not on a worker loop thread. Catches
    a regression like ``await_only(asyncio.to_thread(...))`` that the
    AST pin above guards syntactically and this pin guards
    behaviourally."""
    import threading
    from unittest.mock import MagicMock

    from dqliteclient.exceptions import DqliteConnectionError

    captured_thread: list[int] = []

    adapter = MagicMock()
    adapter.close.side_effect = DqliteConnectionError("transport")
    adapter.force_close_transport = MagicMock(
        side_effect=lambda: captured_thread.append(threading.get_ident())
    )

    caller_tid = threading.get_ident()
    DqliteDialect_aio().do_close(adapter)

    assert captured_thread == [caller_tid], (
        f"force_close_transport must run on the caller's thread "
        f"(sync teardown); captured {captured_thread}, caller {caller_tid}"
    )
