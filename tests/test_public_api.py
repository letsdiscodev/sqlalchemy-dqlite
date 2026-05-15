"""Pin the public surface of each sqlalchemydqlite module.

Every public module declares ``__all__`` listing its public names.
These tests guard against accidental leakage of private helpers and
against drift in the public-class set.
"""

import sqlalchemydqlite
import sqlalchemydqlite.aio
import sqlalchemydqlite.base
import sqlalchemydqlite.requirements


def test_package_all_pins_required_minimum() -> None:
    """``__all__`` must contain ``DqliteDialect`` and ``__version__`` at a
    minimum (PEP 396 public attribute, kept for parity with the other
    dqlite packages).

    Additional symbols are re-exported for SA-dialect-package convention
    parity (every SA-shipped dialect's ``__init__.py`` re-exports the
    type names plus ``Insert``/``insert`` plus a ``dialect`` alias). The
    pinning here is a "must-contain" guard rather than a strict equality
    so a future addition does not require updating this test in lockstep
    — but a private-helper leak still trips
    ``test_star_import_base_does_not_leak_private`` below.
    """
    assert {"DqliteDialect", "__version__"}.issubset(set(sqlalchemydqlite.__all__))


def test_package_all_reexports_sqlite_types() -> None:
    """SA convention (5/5 shipped dialect packages): re-export the
    standard type names from package root so users following the
    cookbook with ``from sqlalchemydqlite import VARCHAR`` succeed.

    See ``.venv/.../sqlite/__init__.py:14-29`` for the reference set.
    """
    expected_types = {
        "BLOB",
        "BOOLEAN",
        "CHAR",
        "DATE",
        "DATETIME",
        "DECIMAL",
        "FLOAT",
        "INTEGER",
        "JSON",
        "NUMERIC",
        "REAL",
        "SMALLINT",
        "TEXT",
        "TIME",
        "TIMESTAMP",
        "VARCHAR",
    }
    assert expected_types.issubset(set(sqlalchemydqlite.__all__))
    for name in expected_types:
        assert hasattr(sqlalchemydqlite, name), f"sqlalchemydqlite.{name} missing"


def test_package_reexports_insert_dml() -> None:
    """SA convention: re-export ``Insert`` / ``insert`` so the ON
    CONFLICT DML constructor is reachable from the package root, same
    way ``sqlalchemy.dialects.sqlite`` re-exports it from
    ``sqlalchemy.dialects.sqlite.dml``."""
    from sqlalchemy.dialects.sqlite import Insert as SQLiteInsert
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    from sqlalchemydqlite import Insert, insert

    # Reference equality preserves single source of truth.
    assert Insert is SQLiteInsert
    assert insert is sqlite_insert


def test_package_dialect_alias_points_at_dqlite_dialect() -> None:
    """SA convention: ``<pkg>.dialect`` is the canonical sync default
    class. Pysqlite at ``.venv/.../sqlite/__init__.py:34`` does
    ``base.dialect = dialect = pysqlite.dialect``."""
    from sqlalchemydqlite import DqliteDialect, dialect

    assert dialect is DqliteDialect


def test_base_all_is_minimal() -> None:
    """``base.__all__`` carries the dialect entry point AND the
    public compiler subclass-extension hook (consumers may extend
    ``DqliteCompiler`` to add custom ``visit_*`` methods, mirroring
    SA's pysqlite-exposed ``SQLiteCompiler``)."""
    assert set(sqlalchemydqlite.base.__all__) == {"DqliteCompiler", "DqliteDialect"}


def test_requirements_all_is_minimal() -> None:
    assert set(sqlalchemydqlite.requirements.__all__) == {"Requirements"}


def test_aio_all_contains_public_adapter_classes() -> None:
    assert {"AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"}.issubset(
        set(sqlalchemydqlite.aio.__all__)
    )


def test_star_import_base_does_not_leak_private() -> None:
    """``from sqlalchemydqlite.base import *`` must not expose private helpers.

    ``_DqliteDateTime``, ``_DqliteDate``, ``_parse_url_bool`` are
    private module-level symbols. A bare ``__all__ = ["DqliteDialect"]``
    guarantees ``import *`` omits them.
    """
    namespace: dict[str, object] = {}
    exec("from sqlalchemydqlite.base import *", namespace)  # noqa: S102
    assert "DqliteDialect" in namespace
    assert "_DqliteDateTime" not in namespace
    assert "_DqliteDate" not in namespace
    assert "_parse_url_bool" not in namespace


def test_star_import_requirements_does_not_leak_private() -> None:
    namespace: dict[str, object] = {}
    exec("from sqlalchemydqlite.requirements import *", namespace)  # noqa: S102
    assert "Requirements" in namespace
    # Defensive: confirm no SuiteRequirements leakage from the import alias path.
    assert "SuiteRequirements" not in namespace
