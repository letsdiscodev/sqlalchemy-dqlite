"""Pin: ``AsyncAdaptedConnection._force_close_transport`` does not
raise ``ReferenceError`` when ``self._connection`` is a dead
``weakref.proxy``.

``AsyncAdaptedConnection._release_inner_strong_ref`` rebinds
``self._connection`` to ``weakref.proxy(self._connection)`` on
``aclose()``. If the inner ``AsyncConnection`` is GC'd between the
swap and a later cleanup pass, attribute access on the proxy raises
``ReferenceError``.

The inherited ``DqliteDialect.do_close`` fallback at
``base.py:2106-2113`` wraps the call to
``dbapi_connection.force_close_transport()`` in
``contextlib.suppress(*_TRANSPORT_CLASS_EXCEPTIONS)``.
``_TRANSPORT_CLASS_EXCEPTIONS`` does NOT include ``ReferenceError``,
so a ``ReferenceError`` escapes the suppress, aborting
``engine.dispose()`` mid-pool-drain and leaking the remaining slots'
transports.

The method's docstring promises "Never raises". This pin keeps the
contract honest by exercising the dead-proxy state directly.
"""

from __future__ import annotations

import logging
import weakref

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_force_close_transport_dead_proxy_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A dead ``weakref.proxy`` on ``self._connection`` must not let
    ``ReferenceError`` escape ``_force_close_transport``.
    """
    # Build a dead proxy: the target goes away immediately so any
    # attribute access on the proxy raises ``ReferenceError``.
    target = type("DeadInner", (), {})()
    proxy = weakref.proxy(target)
    del target  # proxy now points at a GC'd target

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = proxy

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        # Must NOT raise — neither ``ReferenceError`` from the
        # pre-try ``getattr`` reads nor anything else. The docstring
        # contract is "Never raises".
        adapter._force_close_transport()


def test_force_close_transport_dead_proxy_logs_at_debug_with_marker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When the dead-proxy ``ReferenceError`` is swallowed by the
    ``except (Exception, asyncio.CancelledError)`` arm, a DEBUG
    record must fire so the swallow leaves an audit trail. A
    regression that drops the log call would make "swallowed
    silently" operationally indistinguishable from "transport was
    healthy and closed cleanly", contradicting the swallow-and-log
    discipline named in the method's docstring.

    Pin both that a DEBUG record fires AND that it carries an
    identifiable marker (``ReferenceError`` exception type name) so
    operator grep-based monitoring keeps working after rewording."""
    target = type("DeadInner", (), {})()
    proxy = weakref.proxy(target)
    del target

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = proxy

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.aio"):
        adapter._force_close_transport()

    debug = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert debug, "_force_close_transport must log at DEBUG on dead-proxy swallow"
    # The swallow-arm format includes ``type(exc).__name__`` — verify the
    # ReferenceError name landed in the record (not an unrelated DEBUG
    # such as the no-op "hook missing" arm).
    assert any("ReferenceError" in r.message for r in debug), (
        "DEBUG record must carry the ReferenceError class name as an "
        "identifiable marker for grep-based audit trails. Got: "
        f"{[r.message for r in debug]}"
    )


def test_public_force_close_transport_dead_proxy_does_not_raise() -> None:
    """The public alias used by ``DqliteDialect.do_close`` must also
    silently absorb a dead-proxy ``self._connection``.
    """
    target = type("DeadInner", (), {})()
    proxy = weakref.proxy(target)
    del target

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = proxy

    # Must NOT raise.
    adapter.force_close_transport()
