"""Pin: ``AsyncAdaptedConnection.__init__`` rejects the swapped
``(connection, dbapi)`` shape at construction with a naming ``TypeError``,
via a duck-typing guard (first arg has ``OperationalError``, second has
``cursor``) so the misuse doesn't surface as a distant ``AttributeError``.
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
    """The positional swap ``(raw_conn, dbapi_module)`` raises a naming
    ``TypeError`` at construction, not a distant ``AttributeError``."""
    raw: Any = _fake_async_connection()
    mod: Any = _fake_dbapi_module()
    with pytest.raises(TypeError, match="swap"):
        AsyncAdaptedConnection(raw, mod)


def test_init_sa_reference_shape_still_accepted() -> None:
    """The SA reference shape ``(dbapi, connection)`` is still accepted."""
    raw: Any = _fake_async_connection()
    mod: Any = _fake_dbapi_module()
    adapter = AsyncAdaptedConnection(mod, raw)
    assert adapter._connection is raw
    assert adapter.dbapi is mod


def test_init_legacy_single_positional_still_accepted() -> None:
    """The legacy single-positional shape ``(raw)`` is still accepted."""
    raw = _fake_async_connection()
    adapter = AsyncAdaptedConnection(raw)
    assert adapter._connection is raw
    assert adapter.dbapi is None


def test_init_explicit_none_connection_still_accepted() -> None:
    """Explicit ``connection=None`` is accepted; the guard skips the
    ``cursor`` check on a ``None`` inner."""
    mod = _fake_dbapi_module()
    adapter = AsyncAdaptedConnection(mod, connection=None)
    assert adapter._connection is None
    assert adapter.dbapi is mod
