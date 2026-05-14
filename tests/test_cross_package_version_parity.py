"""Cross-package version parity pin.

The four packages (``dqlitewire``, ``dqliteclient``, ``dqlitedbapi``,
``sqlalchemydqlite``) are co-released on the same version cadence —
``sqlalchemydqlite`` declares the other three as runtime dependencies
and is the natural umbrella in the dependency graph. Each package
already has a local ``test_version.py`` that pins its own
``__version__`` against its own ``pyproject.toml``, but no test
catches the case where one package's version is bumped without the
others. This test pins inter-package agreement and lives in the
umbrella package so a release-discipline lapse surfaces at the top
of the stack.

If a future release deliberately desynchronises versions (e.g. a
patch-level wire fix that does not warrant a full stack revision),
this test should be updated to reflect the new contract — not
deleted silently.
"""

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
    """Walk a downstream package's source tree and collect every name
    imported from the curated ``<producer>`` top level (i.e. ``from
    <producer> import X`` — not from any submodule like
    ``<producer>.constants`` / ``<producer>.messages.*``). The bare
    top-level ``from`` form is the documented stable surface; this
    helper feeds the per-producer pin tests below.
    """
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
    """Shared assertion body — every name imported via
    ``from <producer> import X`` across the listed downstream
    packages must resolve at the ``<producer>`` top level.

    A name resolves when either (a) it appears in ``dir(producer)``
    (an attribute / re-export) or (b) ``<producer>.<name>`` is itself
    an importable submodule. Python's ``from pkg import sub`` triggers
    the submodule import even when ``pkg.__init__`` hasn't pre-imported
    ``sub`` — so a downstream's ``from dqlitedbapi import aio`` is a
    valid top-level form even though ``aio`` is a subpackage rather
    than a re-exported name.
    """
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
    """Every ``from dqlitewire import X`` across the three downstream
    packages must resolve against wire's currently-loaded ``dir()``.

    A symbol added to wire after a version bump without lifting the
    downstream's ``dqlite-wire>=X`` floor would still satisfy this
    in-repo test (we always import editable wire here), but it pins
    the curated public-surface invariant: every name downstream code
    spells under the top-level ``from dqlitewire import ...`` form is
    an actual top-level export. The PyPI-install-time floor-match
    half is mitigated by release discipline (always bump wire when
    adding a public symbol consumed downstream).
    """
    _assert_top_level_imports_resolve(dqlitewire, (dqliteclient, dqlitedbapi, sqlalchemydqlite))


def test_client_top_level_imports_resolve_against_current_client() -> None:
    """Sibling pin to the wire test for the next layer up: every
    ``from dqliteclient import X`` across the dbapi and SA packages
    must resolve against client's ``dir()``. A future client public-
    surface rename would otherwise drift unnoticed until import-time
    fails downstream rather than at the producer test suite.
    """
    _assert_top_level_imports_resolve(dqliteclient, (dqlitedbapi, sqlalchemydqlite))


def test_dbapi_top_level_imports_resolve_against_current_dbapi() -> None:
    """Sibling pin to the client test for the next layer up: every
    ``from dqlitedbapi import X`` in the SA package must resolve
    against dbapi's ``dir()``. SA is the only direct consumer.
    """
    _assert_top_level_imports_resolve(dqlitedbapi, (sqlalchemydqlite,))
