"""The intentional divergence between _isolation_lookup (SERIALIZABLE only)
and get_isolation_level_values() (adds AUTOCOMMIT) is documented and
preserved: the values list advertises AUTOCOMMIT to route it to the
dedicated rejection in set_isolation_level."""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


def test_dialect_docstring_documents_isolation_divergence() -> None:
    """The class docstring must explain the two surfaces and their roles."""
    doc = DqliteDialect.__doc__ or ""
    assert "_isolation_lookup" in doc
    assert "get_isolation_level_values" in doc
    assert "AUTOCOMMIT" in doc
    assert "diagnostic" in doc.lower()


def test_isolation_lookup_truthful_set_is_serializable_only() -> None:
    """_isolation_lookup reports only the level the dialect honours."""
    assert set(DqliteDialect._isolation_lookup.keys()) == {"SERIALIZABLE"}


def test_values_list_includes_autocommit_for_routing() -> None:
    """get_isolation_level_values advertises AUTOCOMMIT as the routing
    channel to set_isolation_level's dedicated rejection."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    values = dialect.get_isolation_level_values(MagicMock())
    assert "SERIALIZABLE" in values
    assert "AUTOCOMMIT" in values


def test_set_isolation_level_autocommit_raises_dialect_dedicated_message() -> None:
    """AUTOCOMMIT passes SA's validator (it's advertised) then hits the
    dialect's dedicated rejection in set_isolation_level, not SA's generic
    "Invalid value"."""
    from sqlalchemy.exc import ArgumentError

    dialect = DqliteDialect.__new__(DqliteDialect)
    advertised = dialect.get_isolation_level_values(MagicMock())
    assert "AUTOCOMMIT" in advertised, (
        "AUTOCOMMIT must be advertised so SA's validator does not "
        "reject it before reaching set_isolation_level"
    )

    import pytest

    with pytest.raises(ArgumentError) as excinfo:
        dialect.set_isolation_level(MagicMock(), "AUTOCOMMIT")
    msg = str(excinfo.value)
    assert "dqlite" in msg.lower(), (
        f"AUTOCOMMIT rejection must come from the dialect's "
        f"_AUTOCOMMIT_REJECTION_MSG (the diagnostic-routing channel "
        f"the divergence was added for), not SA's generic validator. "
        f"Got: {msg!r}"
    )
    assert "BEGIN" in msg and "COMMIT" in msg, (
        "Dedicated message must explain the SA-side transaction model"
    )
    assert "Invalid value" not in msg, (
        "If SA's generic 'Invalid value for isolation_level' fires, "
        "AUTOCOMMIT was dropped from get_isolation_level_values() — "
        "the diagnostic-routing divergence regressed"
    )
