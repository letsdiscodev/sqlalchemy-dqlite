"""Pin: ``AsyncAdaptedConnection.force_close_transport`` runs
``_release_inner_strong_ref`` on every exit arm.

The sync dialect's ``do_close`` runs the graceful
``dbapi_connection.close()`` first; on a transport-class failure it
falls back to ``dbapi_connection.force_close_transport()``. The
adapter's ``force_close_transport`` public alias previously
delegated to the inner ``_force_close_transport`` but did NOT swap
the proxy via ``_release_inner_strong_ref`` -- that swap ran only
on the adapter's own outer ``try/finally`` graceful-close path.

Without the swap on the public alias, the dialect-level fallback
(which DqliteDialect_aio inherits intentionally from
DqliteDialect.do_close per ``test_do_close_fallback_uses_sync_transport_teardown``)
left the adapter holding a strong ref to the inner ``AsyncConnection``
past close, pinning attached cursors / pool slots from GC in
exactly the cases where the adapter's normal path is most degraded
(``ReferenceError`` / ``RuntimeError("Event loop is closed")``
during ``engine.dispose()``).

Mirror the discipline on the public alias so EVERY caller of
``force_close_transport`` (dialect fallback, terminate, manual
operator forced-reclaim) gets the release.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> Any:
    """Construct an AsyncAdaptedConnection with the minimum state
    to exercise force_close_transport without dialling."""
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    adapter._connection.force_close_transport = MagicMock()
    return adapter


def test_force_close_transport_calls_release(monkeypatch: pytest.MonkeyPatch) -> None:
    """The public alias must trigger ``_release_inner_strong_ref``
    after delegating to the inner force-close."""
    adapter = _make_adapter()
    tracker = MagicMock()
    monkeypatch.setattr(AsyncAdaptedConnection, "_release_inner_strong_ref", lambda self: tracker())

    adapter.force_close_transport()

    adapter._connection.force_close_transport.assert_called_once_with()
    tracker.assert_called_once_with()


def test_force_close_transport_releases_after_inner_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The inner ``_force_close_transport`` is idempotent and
    swallows every ``Exception`` from the dbapi-level call. Whatever
    state the inner returned in, the release must run. Pin via a
    monkeypatched inner that returns cleanly; the release runs
    immediately after."""
    adapter = _make_adapter()
    inner_tracker = MagicMock()
    release_tracker = MagicMock()
    monkeypatch.setattr(
        AsyncAdaptedConnection, "_force_close_transport", lambda self: inner_tracker()
    )
    monkeypatch.setattr(
        AsyncAdaptedConnection, "_release_inner_strong_ref", lambda self: release_tracker()
    )

    adapter.force_close_transport()

    inner_tracker.assert_called_once_with()
    release_tracker.assert_called_once_with()


def test_force_close_transport_tolerates_release_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the release call itself raises, the inner force-close
    still ran. Tail Exception from the release is suppressed so
    the force-close path is never the source of a re-raise back
    into the dialect's fallback arm."""
    adapter = _make_adapter()

    def _release_raises(self: Any) -> None:
        raise RuntimeError("simulated")

    monkeypatch.setattr(AsyncAdaptedConnection, "_release_inner_strong_ref", _release_raises)

    # No raise -- the suppress(Exception) absorbs the release failure.
    adapter.force_close_transport()

    adapter._connection.force_close_transport.assert_called_once_with()
