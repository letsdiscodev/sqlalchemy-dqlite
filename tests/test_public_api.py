"""Pin the public ``__all__`` surface of each sqlalchemydqlite module."""

import sqlalchemydqlite
import sqlalchemydqlite.aio
import sqlalchemydqlite.base
import sqlalchemydqlite.requirements


def test_package_all_pins_required_minimum() -> None:
    """``__all__`` must contain at least ``DqliteDialect`` and ``__version__``
    (must-contain guard, not strict equality)."""
    assert {"DqliteDialect", "__version__"}.issubset(set(sqlalchemydqlite.__all__))


def test_package_all_reexports_sqlite_types() -> None:
    """SA convention: re-export the standard type names from the package
    root so ``from sqlalchemydqlite import VARCHAR`` works."""
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
    """SA convention: re-export ``Insert`` / ``insert`` so the ON CONFLICT
    DML constructor is reachable from the package root."""
    from sqlalchemy.dialects.sqlite import Insert as SQLiteInsert
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    from sqlalchemydqlite import Insert, insert

    assert Insert is SQLiteInsert
    assert insert is sqlite_insert


def test_package_dialect_alias_points_at_dqlite_dialect() -> None:
    """SA convention: ``<pkg>.dialect`` is the canonical sync default class."""
    from sqlalchemydqlite import DqliteDialect, dialect

    assert dialect is DqliteDialect


def test_base_all_is_minimal() -> None:
    """``base.__all__`` carries the dialect entry point and the public
    ``DqliteCompiler`` subclass-extension hook."""
    assert set(sqlalchemydqlite.base.__all__) == {"DqliteCompiler", "DqliteDialect"}


def test_requirements_all_is_minimal() -> None:
    assert set(sqlalchemydqlite.requirements.__all__) == {"Requirements"}


def test_aio_all_contains_public_adapter_classes() -> None:
    assert {"AsyncAdaptedConnection", "AsyncAdaptedCursor", "DqliteDialect_aio"}.issubset(
        set(sqlalchemydqlite.aio.__all__)
    )


def test_star_import_base_does_not_leak_private() -> None:
    """``from sqlalchemydqlite.base import *`` must not expose private helpers."""
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
    # Confirm no SuiteRequirements leakage from the import alias path.
    assert "SuiteRequirements" not in namespace
