"""Pin: dqlite ignores ``PRAGMA read_uncommitted``.

get_isolation_level returns the constant "SERIALIZABLE" because every statement
goes through Raft; if dqlite grows a weaker mode this fails and signals that
get_isolation_level[_values] must start introspecting.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text


@pytest.mark.integration
def test_pragma_read_uncommitted_cannot_weaken_isolation(engine_url: str) -> None:
    """Either dqlite rejects the PRAGMA ("not authorized") or returns 0; both pin
    that a caller cannot weaken isolation. A non-zero result means it can."""
    from sqlalchemy.exc import DatabaseError

    engine = create_engine(engine_url)
    try:
        with engine.connect() as conn:
            try:
                result = conn.execute(text("PRAGMA read_uncommitted")).scalar()
            except DatabaseError as exc:
                # dqlite's authorizer rejects with SQLITE_AUTH (23), which routes
                # to DatabaseError per stdlib parity — stronger than "returns 0".
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
