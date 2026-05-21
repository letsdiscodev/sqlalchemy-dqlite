"""Pin: ``AsyncAdaptedConnection.__init__`` distinguishes the three
call shapes via a module-private sentinel.

Call shapes and their assignment to ``_connection`` / ``dbapi``:

* ``AsyncAdaptedConnection(mod, raw)`` -> ``_connection=raw, dbapi=mod``
* ``AsyncAdaptedConnection(raw)`` -> ``_connection=raw, dbapi=None``
* ``AsyncAdaptedConnection(mod, connection=None)``
  -> ``_connection=None, dbapi=mod`` (the row that traps the
  ``connection is None`` discriminator).

The earlier discriminator was ``connection is None``, which conflated
the second and third rows: an explicit ``connection=None`` (caller
wants the module bound but the inner connection deferred) silently
assigned the dbapi module to ``self._connection``. The sentinel
fix distinguishes "omitted" from "explicit None" and keeps the
public signature stable for IDE auto-complete.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_init_full_sa_reference_shape() -> None:
    """Row 1: ``AsyncAdaptedConnection(mod, raw)`` binds both."""
    mod = MagicMock(name="dbapi_module")
    raw = MagicMock(name="raw_conn")
    adapter = AsyncAdaptedConnection(mod, raw)
    assert adapter._connection is raw
    assert adapter.dbapi is mod


def test_init_legacy_single_positional_shape() -> None:
    """Row 2: ``AsyncAdaptedConnection(raw)`` binds ``_connection``
    only; ``dbapi`` stays ``None``."""
    raw = MagicMock(name="raw_conn")
    adapter = AsyncAdaptedConnection(raw)
    assert adapter._connection is raw
    assert adapter.dbapi is None


def test_init_new_shape_with_explicit_connection_none() -> None:
    """Row 3 — the trap the earlier discriminator hit. An explicit
    ``connection=None`` (deferred inner connection) MUST NOT
    misassign the dbapi module to ``_connection``."""
    mod = MagicMock(name="dbapi_module")
    adapter = AsyncAdaptedConnection(mod, connection=None)
    assert adapter._connection is None, (
        "explicit connection=None must NOT alias to the dbapi module"
    )
    assert adapter.dbapi is mod


def test_init_legacy_single_positional_with_none_value() -> None:
    """Pre-existing in-tree usage: ``AsyncAdaptedConnection(None)``
    (test fixture shape from ``test_handle_exception_must_raise``).
    The second positional is omitted, so the legacy branch fires;
    both slots end up ``None``."""
    adapter = AsyncAdaptedConnection(None)
    assert adapter._connection is None
    assert adapter.dbapi is None
