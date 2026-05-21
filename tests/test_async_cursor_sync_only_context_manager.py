"""Pin: ``AsyncAdaptedCursor`` exposes only the SYNC context-manager /
iterator protocol — ``__enter__`` / ``__exit__`` / ``__iter__`` /
``__next__``. The async counterparts (``__aenter__`` / ``__aexit__`` /
``__aiter__`` / ``__anext__``) are deliberately ABSENT to mirror SA's
reference ``AsyncAdapt_dbapi_cursor`` shape.

The docstring must name the design choice so a future contributor
adding the async surface for cross-driver aiosqlite-style parity is
aware they would diverge from SA's adapter contract.
"""

from __future__ import annotations

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def test_sync_context_manager_present() -> None:
    assert hasattr(AsyncAdaptedCursor, "__enter__")
    assert hasattr(AsyncAdaptedCursor, "__exit__")


def test_async_context_manager_absent() -> None:
    """SA's ``AsyncAdapt_dbapi_cursor`` reference is sync-only — match
    that shape so cross-driver SA-pattern callers see consistent
    behaviour."""
    assert not hasattr(AsyncAdaptedCursor, "__aenter__"), (
        "AsyncAdaptedCursor must not expose __aenter__; SA's reference "
        "AsyncAdapt_dbapi_cursor is sync-only at the cursor layer."
    )
    assert not hasattr(AsyncAdaptedCursor, "__aexit__"), (
        "AsyncAdaptedCursor must not expose __aexit__; SA's reference "
        "AsyncAdapt_dbapi_cursor is sync-only at the cursor layer."
    )


def test_async_iterator_protocol_absent() -> None:
    """Same rationale as __aenter__ — ``async for row in cur:`` is not
    supported; users must reach the underlying dbapi cursor for native
    async iteration."""
    assert not hasattr(AsyncAdaptedCursor, "__aiter__")
    assert not hasattr(AsyncAdaptedCursor, "__anext__")


def test_class_docstring_names_the_sync_only_design_choice() -> None:
    """Pin the documentation so a future contributor reading the class
    docstring sees the intentional SA-reference mirror and the escape
    hatch for native async semantics."""
    doc = AsyncAdaptedCursor.__doc__ or ""
    assert "__aenter__" in doc
    assert "__aiter__" in doc
    assert "AsyncAdapt_dbapi_cursor" in doc
