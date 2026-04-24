"""Pins the invariant that dqlite ignores ``PRAGMA read_uncommitted``.

``DqliteDialect.get_isolation_level`` returns the literal
``"SERIALIZABLE"`` without consulting the connection because dqlite
has no mechanism to weaken isolation — every statement goes through
Raft consensus. If upstream dqlite ever grows a read-replica or
snapshot-isolation mode, this test fails loudly and signals that
``get_isolation_level`` (and ``get_isolation_level_values``) must
start introspecting rather than returning a constant. See ISSUE-651.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text


@pytest.mark.integration
def test_pragma_read_uncommitted_cannot_weaken_isolation(engine_url: str) -> None:
    """``PRAGMA read_uncommitted`` cannot weaken isolation on dqlite.
    Upstream either rejects the PRAGMA with "not authorized" (the
    current behaviour) or returns ``0`` (the inherited stdlib sqlite3
    default-in-shared-cache-mode read behaviour). Either outcome pins
    the invariant: a caller asking for weaker isolation does NOT get
    it.

    If this ever returns a non-zero value without raising, dqlite has
    acquired a weaker isolation mode and ``DqliteDialect."
    get_isolation_level`` / ``get_isolation_level_values`` must stop
    returning a constant and start introspecting.
    """
    from sqlalchemy.exc import OperationalError

    engine = create_engine(engine_url)
    try:
        with engine.connect() as conn:
            try:
                result = conn.execute(text("PRAGMA read_uncommitted")).scalar()
            except OperationalError as exc:
                # dqlite's authorizer currently rejects the PRAGMA
                # outright. That's a stronger invariant than "returns
                # 0" — weaker isolation literally cannot be requested.
                assert "not authorized" in str(exc).lower(), (
                    f"PRAGMA rejected but with unexpected message: {exc}"
                )
                return
            assert result == 0, (
                f"dqlite returned {result!r} for PRAGMA read_uncommitted; "
                "if this is non-zero, isolation has been weakened and "
                "DqliteDialect.get_isolation_level must introspect."
            )
    finally:
        engine.dispose()
