"""Pin: ``AsyncAdaptedCursor`` is sync-only — no async protocol methods,
mirroring SA's ``AsyncAdapt_dbapi_cursor``."""

from __future__ import annotations

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_sync_context_manager_present() -> None:
    assert hasattr(AsyncAdaptedCursor, "__enter__")
    assert hasattr(AsyncAdaptedCursor, "__exit__")


def test_async_context_manager_absent() -> None:
    assert not hasattr(AsyncAdaptedCursor, "__aenter__"), (
        "AsyncAdaptedCursor must not expose __aenter__; SA's reference "
        "AsyncAdapt_dbapi_cursor is sync-only at the cursor layer."
    )
    assert not hasattr(AsyncAdaptedCursor, "__aexit__"), (
        "AsyncAdaptedCursor must not expose __aexit__; SA's reference "
        "AsyncAdapt_dbapi_cursor is sync-only at the cursor layer."
    )


def test_async_iterator_protocol_absent() -> None:
    assert not hasattr(AsyncAdaptedCursor, "__aiter__")
    assert not hasattr(AsyncAdaptedCursor, "__anext__")
