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


def _wire_top_level_names_imported_by(downstream: ModuleType) -> set[str]:
    """Walk a downstream package's source tree and collect every name
    imported from the curated ``dqlitewire`` top level (i.e. ``from
    dqlitewire import X`` — not ``dqlitewire.constants`` /
    ``dqlitewire.messages.*`` / ``dqlitewire._truncate``). The "from
    dqlitewire import X" form is the documented stable surface; this
    test pins each name in that form to wire's current ``dir()``.
    """
    src_root = pathlib.Path(next(iter(downstream.__path__)))
    names: set[str] = set()
    for path in src_root.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != "dqlitewire":
                continue
            for alias in node.names:
                names.add(alias.name)
    return names


def test_wire_top_level_imports_resolve_against_current_wire() -> None:
    """Lighter half of the version-floor check: every name imported via
    ``from dqlitewire import X`` across the three downstream packages
    must be a member of the currently-loaded ``dqlitewire`` module.

    A symbol added to wire after a version bump without lifting the
    downstream's ``dqlite-wire>=X`` floor would still satisfy this
    in-repo test (we always import editable wire here), but it pins
    the curated public-surface invariant: every name downstream code
    spells under the top-level ``from dqlitewire import ...`` form is
    an actual top-level export. The PyPI-install-time floor-match
    half is mitigated by release discipline (always bump wire when
    adding a public symbol consumed downstream).
    """
    available = set(dir(dqlitewire))
    for downstream in (dqliteclient, dqlitedbapi, sqlalchemydqlite):
        expected = _wire_top_level_names_imported_by(downstream)
        missing = expected - available
        assert not missing, (
            f"{downstream.__name__} imports {sorted(missing)!r} via "
            f"``from dqlitewire import ...`` but the names are not on "
            f"the wire top level. Either add them to "
            f"``dqlitewire/__init__.py``'s re-export block (and "
            f"``__all__``) or change the downstream import to the "
            f"submodule it actually lives in."
        )
