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
