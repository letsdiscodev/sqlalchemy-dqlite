"""Pin: ``AsyncAdaptedConnection.__init__`` distinguishes its three call
shapes via a sentinel. A naive ``connection is None`` discriminator
conflates the legacy single-positional shape with an explicit
``connection=None``, misassigning the dbapi module to ``_connection``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_init_full_sa_reference_shape() -> None:
    """``AsyncAdaptedConnection(mod, raw)`` binds both."""
    mod = MagicMock(name="dbapi_module")
    raw = MagicMock(name="raw_conn")
    adapter = AsyncAdaptedConnection(mod, raw)
    assert adapter._connection is raw
    assert adapter.dbapi is mod


def test_init_legacy_single_positional_shape() -> None:
    """``AsyncAdaptedConnection(raw)`` binds ``_connection`` only;
    ``dbapi`` stays ``None``."""
    raw = MagicMock(name="raw_conn")
    adapter = AsyncAdaptedConnection(raw)
    assert adapter._connection is raw
    assert adapter.dbapi is None


def test_init_new_shape_with_explicit_connection_none() -> None:
    """Explicit ``connection=None`` must not misassign the dbapi module
    to ``_connection`` (the trap the earlier discriminator hit)."""
    mod = MagicMock(name="dbapi_module")
    adapter = AsyncAdaptedConnection(mod, connection=None)
    assert adapter._connection is None, (
        "explicit connection=None must NOT alias to the dbapi module"
    )
    assert adapter.dbapi is mod


def test_init_legacy_single_positional_with_none_value() -> None:
    """``AsyncAdaptedConnection(None)``: second positional omitted, so the
    legacy branch fires and both slots end up ``None``."""
    adapter = AsyncAdaptedConnection(None)
    assert adapter._connection is None
    assert adapter.dbapi is None
