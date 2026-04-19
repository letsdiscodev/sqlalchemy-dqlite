"""Pin the public surface of each sqlalchemydqlite module.

Every public module declares ``__all__`` listing its public names.
These tests guard against accidental leakage of private helpers and
against drift in the public-class set. See ISSUE-112 / 131 / 180
precedents for the policy.
"""

import sqlalchemydqlite
import sqlalchemydqlite.aio
import sqlalchemydqlite.base
import sqlalchemydqlite.requirements


def test_package_all_is_minimal() -> None:
    assert set(sqlalchemydqlite.__all__) == {"DqliteDialect"}


def test_base_all_is_minimal() -> None:
    assert set(sqlalchemydqlite.base.__all__) == {"DqliteDialect"}


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
