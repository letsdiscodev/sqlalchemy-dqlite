"""Each ``sqlalchemydqlite`` submodule must declare ``__all__`` so
``from sqlalchemydqlite.<sub> import *`` does not leak private helpers.

Mirrors the harness in ``dqlitewire``, ``dqliteclient``, and
``dqlitedbapi``. Without per-submodule ``__all__``, a star-import
leaks ``logging``, SA internals, etc.
"""

from __future__ import annotations

import importlib

import pytest

_SUBMODULES = [
    "sqlalchemydqlite",
    "sqlalchemydqlite.base",
    "sqlalchemydqlite.aio",
    "sqlalchemydqlite.requirements",
    "sqlalchemydqlite.provision",
]


@pytest.mark.parametrize("modname", _SUBMODULES)
def test_submodule_declares_all(modname: str) -> None:
    mod = importlib.import_module(modname)
    assert hasattr(mod, "__all__"), f"{modname} is missing __all__"
    exported = mod.__all__
    assert isinstance(exported, list | tuple), (
        f"{modname}.__all__ must be list/tuple, got {type(exported).__name__}"
    )
    for name in exported:
        assert isinstance(name, str), f"{modname}.__all__ entries must be strings; got {name!r}"
        assert hasattr(mod, name), f"{modname}.__all__ lists {name!r} but it is not defined"
