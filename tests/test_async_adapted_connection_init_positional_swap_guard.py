"""Pin: ``AsyncAdaptedConnection.__init__`` rejects the
``(connection, dbapi)`` positional swap immediately at construction
with a ``TypeError`` naming the misuse.

The ``_UNSET`` sentinel distinguishes the SA reference shape
``(dbapi, connection)`` from the legacy single-positional shape
``(connection,)``, but without a shape guard it silently accepts a
third plausible shape — the swapped ``(connection, dbapi)`` — and
poisons ``self._connection`` / ``self.dbapi``. The downstream
``AttributeError`` then surfaces many frames from the construction
site. A cheap duck-typing guard catches the swap at construction:
the first positional must look like a dbapi module
(have ``OperationalError``); the second positional must look like
an ``AsyncConnection`` (have ``cursor``).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _fake_dbapi_module() -> SimpleNamespace:
    return SimpleNamespace(
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
    )


def _fake_async_connection() -> SimpleNamespace:
    return SimpleNamespace(cursor=lambda: None)


def test_init_swapped_positional_raises_typeerror() -> None:
    """``AsyncAdaptedConnection(raw_conn, dbapi_module)`` — the
    positional swap — surfaces a ``TypeError`` at construction
    naming the misuse. Without the guard, the misassignment is
    silent and the ``AttributeError`` surfaces many frames later.
    """
    raw: Any = _fake_async_connection()
    mod: Any = _fake_dbapi_module()
    with pytest.raises(TypeError, match="swap"):
        AsyncAdaptedConnection(raw, mod)


def test_init_sa_reference_shape_still_accepted() -> None:
    """``AsyncAdaptedConnection(dbapi, connection)`` — the SA
    reference shape — remains accepted unchanged."""
    raw: Any = _fake_async_connection()
    mod: Any = _fake_dbapi_module()
    adapter = AsyncAdaptedConnection(mod, raw)
    assert adapter._connection is raw
    assert adapter.dbapi is mod


def test_init_legacy_single_positional_still_accepted() -> None:
    """``AsyncAdaptedConnection(raw)`` — legacy single positional
    — remains accepted unchanged."""
    raw = _fake_async_connection()
    adapter = AsyncAdaptedConnection(raw)
    assert adapter._connection is raw
    assert adapter.dbapi is None


def test_init_explicit_none_connection_still_accepted() -> None:
    """``AsyncAdaptedConnection(dbapi, connection=None)`` — explicit
    deferred inner connection — remains accepted; the guard skips
    the ``cursor`` check on a ``None`` inner."""
    mod = _fake_dbapi_module()
    adapter = AsyncAdaptedConnection(mod, connection=None)
    assert adapter._connection is None
    assert adapter.dbapi is mod
