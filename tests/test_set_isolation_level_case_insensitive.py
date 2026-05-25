"""Pin: ``DqliteDialect.set_isolation_level`` is case-insensitive
on the level argument.

SA's engine flow upper-cases the level before dispatching to the
dialect hook, but direct callers (test harnesses, custom engine
implementations, third-party connect-listener authors) often pass
``"serializable"`` / mixed-case forms straight. The previous
case-sensitive comparison fell through such inputs to the generic
rejection message, which was confusing because the value only
differed in case.

The AUTOCOMMIT rejection arm is also case-insensitive so the
sibling discipline ``__init__`` already enforces (``iso_level.upper()
== "AUTOCOMMIT"``) is uniform across both entry points.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "level",
    ["SERIALIZABLE", "serializable", "Serializable", "sErIaLiZaBlE"],
)
def test_set_isolation_level_serializable_case_insensitive(level: str) -> None:
    """Any case form of SERIALIZABLE is accepted as no-op."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    # No exception raised — successful no-op acknowledgement.
    dialect.set_isolation_level(conn, level)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "level",
    ["AUTOCOMMIT", "autocommit", "Autocommit"],
)
def test_set_isolation_level_autocommit_rejected_case_insensitive(level: str) -> None:
    """Any case form of AUTOCOMMIT routes through the dedicated
    rejection (sibling to ``__init__``'s symmetric arm)."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        dialect.set_isolation_level(conn, level)  # type: ignore[arg-type]


def test_set_isolation_level_unknown_string_rejected() -> None:
    """Unknown levels still hit the generic rejection. The level is
    reported verbatim (not upper-cased) so the operator sees what
    they passed."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match=r"requested level 'READ COMMITTED'"):
        dialect.set_isolation_level(conn, "READ COMMITTED")


def test_set_isolation_level_none_rejected() -> None:
    """``None`` is rejected with ``ArgumentError`` matching pysqlite
    parent's ``KeyError``-on-None behaviour. The ``reset_isolation_level``
    no-op override at the dialect level disconnects the SA-internal
    reset path from this method, so the earlier silent-accept-None
    arm is no longer reachable from any internal caller."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match="non-None"):
        dialect.set_isolation_level(conn, None)  # type: ignore[arg-type]


def test_set_isolation_level_non_string_rejected() -> None:
    """A non-string value (numeric, list, etc.) raises ArgumentError
    rather than crashing in ``.upper()``."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    conn = MagicMock()
    with pytest.raises(ArgumentError, match="not a string"):
        dialect.set_isolation_level(conn, 42)  # type: ignore[arg-type]
