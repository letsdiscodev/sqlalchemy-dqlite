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
