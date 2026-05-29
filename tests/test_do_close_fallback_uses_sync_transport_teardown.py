"""Pin: ``do_close``'s fallback reaches ``force_close_transport`` (sync
teardown) on both sync and async dialects — by the time it fires the async
close already failed, so async-native teardown would just fail again.
"""

from __future__ import annotations

import inspect

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_async_dialect_inherits_do_close_intentionally() -> None:
    """The async dialect does NOT override ``do_close`` (inherited)."""
    assert "do_close" not in DqliteDialect_aio.__dict__, (
        "DqliteDialect_aio inherits do_close from the sync dialect; "
        "the inherited fallback reaches force_close_transport (sync) "
        "which is the documented sync-teardown surface — see the "
        "sync do_close docstring for rationale."
    )


def test_do_close_body_does_not_bounce_through_await_only_or_greenlet() -> None:
    """The fallback must NOT route through await_only / greenlet_spawn /
    asyncio.to_thread — it runs synchronously on the SA-pool thread."""
    src = inspect.getsource(DqliteDialect.do_close)
    assert "await_only" not in src, (
        "DqliteDialect.do_close (inherited by async dialect) must NOT "
        "use await_only on the fallback leg — sync teardown is "
        "intentional on both dialects."
    )
    assert "greenlet_spawn" not in src
    assert "to_thread" not in src
    assert DqliteDialect_aio.do_close is DqliteDialect.do_close


def test_async_do_close_fallback_runs_in_calling_thread_not_loop() -> None:
    """``force_close_transport`` must run on the caller's thread, not a
    worker loop thread."""
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
