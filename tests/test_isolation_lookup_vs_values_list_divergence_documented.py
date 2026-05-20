"""Pin: the deliberate divergence between ``_isolation_lookup``
(``{"SERIALIZABLE": 0}``) and ``get_isolation_level_values()``
(``["SERIALIZABLE", "AUTOCOMMIT"]``) is documented in the dialect
class docstring, and the divergence is preserved.

The divergence is intentional and load-bearing:

* ``_isolation_lookup`` is the truthful single-entry surface for
  introspection.
* ``get_isolation_level_values()`` advertises ``AUTOCOMMIT`` as a
  diagnostic-routing channel so SA's validator passes the value
  through to ``set_isolation_level``, which raises the dedicated
  ``_AUTOCOMMIT_REJECTION_MSG``.

A future change that "fixes the inconsistency" by aligning the
two surfaces would either (a) re-introduce ``AUTOCOMMIT`` as a
honoured level (data-loss footgun), or (b) drop the diagnostic-
routing entry (operator-confusing rejection message).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


def test_dialect_docstring_documents_isolation_divergence() -> None:
    """The class docstring must explicitly explain the two surfaces
    and their roles."""
    doc = DqliteDialect.__doc__ or ""
    assert "_isolation_lookup" in doc
    assert "get_isolation_level_values" in doc
    assert "AUTOCOMMIT" in doc
    assert "diagnostic" in doc.lower()


def test_isolation_lookup_truthful_set_is_serializable_only() -> None:
    """``_isolation_lookup`` reports the level the dialect honours."""
    assert set(DqliteDialect._isolation_lookup.keys()) == {"SERIALIZABLE"}


def test_values_list_includes_autocommit_for_routing() -> None:
    """``get_isolation_level_values`` advertises ``AUTOCOMMIT`` as the
    routing channel to ``set_isolation_level``'s dedicated rejection."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    values = dialect.get_isolation_level_values(MagicMock())
    assert "SERIALIZABLE" in values
    assert "AUTOCOMMIT" in values


def test_set_isolation_level_autocommit_raises_dialect_dedicated_message() -> None:
    """End-to-end semantic pin: when SA's
    ``_assert_and_set_isolation_level`` validates the level against
    ``get_isolation_level_values()`` (which includes AUTOCOMMIT) and
    then forwards to ``set_isolation_level``, the dialect's
    dedicated rejection message fires — naming the single-leader
    Raft / SA transaction-model rationale.

    A regression that "aligns the inconsistency" by dropping
    AUTOCOMMIT from ``get_isolation_level_values()`` would cause
    SA's generic "Invalid value" validator to fire instead,
    masking the dialect-specific operator guidance. The negative
    assertion below catches that scenario."""
    from sqlalchemy.exc import ArgumentError

    dialect = DqliteDialect.__new__(DqliteDialect)
    # Confirm AUTOCOMMIT routes through the validator (it's in the values list).
    advertised = dialect.get_isolation_level_values(MagicMock())
    assert "AUTOCOMMIT" in advertised, (
        "AUTOCOMMIT must be advertised so SA's validator does not "
        "reject it before reaching set_isolation_level"
    )

    # The dedicated rejection fires with the dqlite-specific rationale.
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
    # Negative: SA's generic validator message must NOT have surfaced.
    assert "Invalid value" not in msg, (
        "If SA's generic 'Invalid value for isolation_level' fires, "
        "AUTOCOMMIT was dropped from get_isolation_level_values() — "
        "the diagnostic-routing divergence regressed"
    )
