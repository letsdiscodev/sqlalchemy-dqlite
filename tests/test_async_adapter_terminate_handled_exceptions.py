"""Pin: ``AsyncAdaptedConnection._terminate_handled_exceptions()`` exposes
the catch tuple that the hand-rolled ``terminate()`` body uses.

Mirrors SA's reference at ``sqlalchemy/connectors/asyncio.py:417-421``.
Third-party SA-async instrumentation (Sentry async-pool wrapper,
sqlalchemy-utils diagnostics) introspects this hook on the connection
adapter; without it those tools fall through to a less-informative path
where the equivalent attribute exists on every other async SA dialect.

Pure introspection parity. Does not change runtime behaviour. The
relationship to wont-fix ISSUE-1311 is documented in ``aio.py``.
"""

from __future__ import annotations

import asyncio

from sqlalchemydqlite.aio import AsyncAdaptedConnection
from sqlalchemydqlite.base import _TRANSPORT_CLASS_EXCEPTIONS


def test_terminate_handled_exceptions_method_exists() -> None:
    assert hasattr(AsyncAdaptedConnection, "_terminate_handled_exceptions")


def test_terminate_handled_exceptions_returns_expected_tuple() -> None:
    handled = AsyncAdaptedConnection._terminate_handled_exceptions()
    assert isinstance(handled, tuple)
    # All transport-class exceptions are present.
    for cls in _TRANSPORT_CLASS_EXCEPTIONS:
        assert cls in handled
    # CancelledError is also handled (the explicit ``CancelledError``
    # arm in the hand-rolled terminate body).
    assert asyncio.CancelledError in handled
