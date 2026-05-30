"""Pin: ``do_close``'s fallback reaches ``force_close_transport`` (sync
teardown) on both sync and async dialects — by the time it fires the async
close already failed, so async-native teardown would just fail again.
"""

from __future__ import annotations

from sqlalchemydqlite.aio import DqliteDialect_aio


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
