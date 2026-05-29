"""Pin: the public ``force_close_transport`` alias runs ``_release_inner_strong_ref`` on every
exit arm, so the dialect-fallback path does not leave the adapter pinning the inner connection."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> Any:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    adapter._connection.force_close_transport = MagicMock()
    return adapter


def test_force_close_transport_calls_release(monkeypatch: pytest.MonkeyPatch) -> None:
    """The public alias triggers ``_release_inner_strong_ref`` after the inner force-close."""
    adapter = _make_adapter()
    tracker = MagicMock()
    monkeypatch.setattr(AsyncAdaptedConnection, "_release_inner_strong_ref", lambda self: tracker())

    adapter.force_close_transport()

    adapter._connection.force_close_transport.assert_called_once_with()
    tracker.assert_called_once_with()


def test_force_close_transport_releases_after_inner_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whatever state the inner force-close returned in, the release must run after it."""
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
    """A tail Exception from the release is suppressed so force-close never re-raises."""
    adapter = _make_adapter()

    def _release_raises(self: Any) -> None:
        raise RuntimeError("simulated")

    monkeypatch.setattr(AsyncAdaptedConnection, "_release_inner_strong_ref", _release_raises)

    adapter.force_close_transport()

    adapter._connection.force_close_transport.assert_called_once_with()
