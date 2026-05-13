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
