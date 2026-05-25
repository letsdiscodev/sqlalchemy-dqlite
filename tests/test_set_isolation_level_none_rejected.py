"""Pin: ``DqliteDialect.set_isolation_level(conn, None)`` raises
``ArgumentError`` (pysqlite-parity strict rejection) instead of the
earlier silent no-op.

The previous silent-accept-None arm protected ``DefaultDialect.
reset_isolation_level`` calling through with
``self.default_isolation_level=None`` on a dialect that skipped
``initialize()``. The dialect now overrides ``reset_isolation_level``
to a local no-op, so the harness-bypass path no longer reaches
``set_isolation_level`` at all. Mirroring pysqlite's
``_isolation_lookup[None]`` ``KeyError`` with an SA-shaped
``ArgumentError`` removes the divergence while keeping the
``reset_isolation_level`` harness-bypass behaviour intact (via the
local no-op).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_set_isolation_level_none_raises_argument_error() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="non-None"):
        dialect.set_isolation_level(MagicMock(), None)  # type: ignore[arg-type]


def test_reset_isolation_level_still_callable_on_uninitialised_dialect() -> None:
    """The harness-bypass path the silent-accept-None arm was
    documented to protect now lives in ``reset_isolation_level``'s
    no-op override — verify it still works without raising even when
    ``initialize()`` has not been called."""
    dialect = DqliteDialect()
    # Must NOT raise.
    dialect.reset_isolation_level(MagicMock())


def test_set_isolation_level_serializable_still_accepted() -> None:
    """Defensive: the strict-reject-None change must not affect the
    documented accept path."""
    dialect = DqliteDialect()
    # No raise — SERIALIZABLE is the only accepted level.
    dialect.set_isolation_level(MagicMock(), "SERIALIZABLE")
