"""Cross-package version parity pin: the four co-released packages must
share one version. Lives in the umbrella package (sqlalchemydqlite). If a
release deliberately desyncs versions, update this contract, don't delete."""

from __future__ import annotations

import ast
import pathlib
from types import ModuleType

import dqliteclient
import dqlitedbapi
import dqlitewire
import sqlalchemydqlite


def test_all_four_packages_share_the_same_version() -> None:
    wire_v = dqlitewire.__version__
    client_v = dqliteclient.__version__
    dbapi_v = dqlitedbapi.__version__
    sa_v = sqlalchemydqlite.__version__

    assert wire_v == client_v == dbapi_v == sa_v, (
        f"Cross-package version drift: "
        f"dqlitewire={wire_v!r}, dqliteclient={client_v!r}, "
        f"dqlitedbapi={dbapi_v!r}, sqlalchemydqlite={sa_v!r}. "
        f"Bump all four together when releasing."
    )


def _top_level_names_imported_by(downstream: ModuleType, producer: str) -> set[str]:
    """Names imported via bare ``from <producer> import X`` (not from
    submodules) across a downstream package's source tree."""
    src_root = pathlib.Path(next(iter(downstream.__path__)))
    names: set[str] = set()
    for path in src_root.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != producer:
                continue
            for alias in node.names:
                names.add(alias.name)
    return names


def _assert_top_level_imports_resolve(
    producer: ModuleType,
    downstreams: tuple[ModuleType, ...],
) -> None:
    """Every ``from <producer> import X`` name must resolve: either in
    ``dir(producer)`` or as an importable submodule (``from pkg import
    sub`` triggers the submodule import even without a re-export)."""
    import importlib

    available = set(dir(producer))
    for downstream in downstreams:
        expected = _top_level_names_imported_by(downstream, producer.__name__)
        truly_missing: set[str] = set()
        for name in expected - available:
            try:
                importlib.import_module(f"{producer.__name__}.{name}")
            except ModuleNotFoundError:
                truly_missing.add(name)
        assert not truly_missing, (
            f"{downstream.__name__} imports {sorted(truly_missing)!r} via "
            f"``from {producer.__name__} import ...`` but the names are "
            f"neither on the {producer.__name__} top level nor importable "
            f"as submodules. Either add them to "
            f"``{producer.__name__}/__init__.py``'s re-export block "
            f"(and ``__all__``) or change the downstream import to the "
            f"submodule it actually lives in."
        )


def test_wire_top_level_imports_resolve_against_current_wire() -> None:
    """Every ``from dqlitewire import X`` across the downstream packages
    must resolve against wire's top level. Pins the public-surface
    invariant only; the editable import can't catch a missing >= floor."""
    _assert_top_level_imports_resolve(dqlitewire, (dqliteclient, dqlitedbapi, sqlalchemydqlite))


def test_client_top_level_imports_resolve_against_current_client() -> None:
    """Every ``from dqliteclient import X`` across dbapi and SA must
    resolve against client's top level."""
    _assert_top_level_imports_resolve(dqliteclient, (dqlitedbapi, sqlalchemydqlite))


def test_dbapi_top_level_imports_resolve_against_current_dbapi() -> None:
    """Every ``from dqlitedbapi import X`` in SA must resolve against
    dbapi's top level."""
    _assert_top_level_imports_resolve(dqlitedbapi, (sqlalchemydqlite,))
