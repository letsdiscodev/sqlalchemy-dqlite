"""``_force_close_transport`` must not raise ``ReferenceError`` when
``self._connection`` is a dead ``weakref.proxy``: the inherited
do_close suppress tuple excludes it, so it would abort dispose()."""

from __future__ import annotations

import logging
import weakref

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_force_close_transport_dead_proxy_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = type("DeadInner", (), {})()
    proxy = weakref.proxy(target)
    del target  # proxy now points at a GC'd target

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = proxy

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        adapter._force_close_transport()


def test_force_close_transport_dead_proxy_logs_at_debug_with_marker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The swallowed dead-proxy ReferenceError must leave a DEBUG audit
    trail carrying the exception type name for grep-based monitoring."""
    target = type("DeadInner", (), {})()
    proxy = weakref.proxy(target)
    del target

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = proxy

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        adapter._force_close_transport()

    debug = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert debug, "_force_close_transport must log at DEBUG on dead-proxy swallow"
    assert any("ReferenceError" in r.message for r in debug), (
        "DEBUG record must carry the ReferenceError class name as an "
        "identifiable marker for grep-based audit trails. Got: "
        f"{[r.message for r in debug]}"
    )


def test_public_force_close_transport_dead_proxy_does_not_raise() -> None:
    """The public alias used by ``DqliteDialect.do_close`` must also
    silently absorb a dead-proxy ``self._connection``."""
    target = type("DeadInner", (), {})()
    proxy = weakref.proxy(target)
    del target

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = proxy

    adapter.force_close_transport()
