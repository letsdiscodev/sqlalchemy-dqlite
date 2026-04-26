"""SA's ``SQLiteDialect.__init__`` writes instance attributes based on
``sqlite_version_info`` and ``util.pypy``. Without a
``DqliteDialect.__init__`` override that re-pins the flags, those
instance writes shadow our class-level pins — on PyPy in particular,
``insert_returning`` / ``update_returning`` / ``delete_returning``
are unconditionally zeroed.

Pin that every `DqliteDialect()` instance has the expected values
as **instance attributes** (not just class attributes).
"""

from __future__ import annotations

import pytest

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
@pytest.mark.parametrize(
    "flag",
    [
        "insert_returning",
        "update_returning",
        "delete_returning",
        "supports_default_values",
        "supports_multivalues_insert",
    ],
)
def test_dialect_instance_pins_survive_parent_init(cls: type, flag: str) -> None:
    d = cls()
    # Instance attribute present (not just class default)
    assert flag in vars(d), (
        f"{cls.__name__}.{flag} is not an instance attribute — SA's "
        f"SQLiteDialect.__init__ overwrites at instance level, so the "
        f"pin must live at instance level to survive it."
    )
    assert getattr(d, flag) is True, (
        f"{cls.__name__}.{flag} is {getattr(d, flag)!r} on the instance "
        f"(class-level pin was silently shadowed)"
    )


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_insertmanyvalues_max_parameters_not_capped(cls: type) -> None:
    """Regression guard: parent's version-gated ``< (3, 32, 0)`` write
    to this attribute must not leak into dqlite's instance config. The
    ``__init__`` re-applies the DefaultDialect value at instance scope
    so a future parent gate extension (e.g. ``or util.pypy``
    symmetric to the RETURNING branch) cannot silently cap batch
    INSERTs at 999 parameters.
    """
    d = cls()
    assert "insertmanyvalues_max_parameters" in vars(d), (
        f"{cls.__name__}.insertmanyvalues_max_parameters must live at "
        f"instance level so the parent's version-gated write cannot "
        f"silently shadow it."
    )
    assert d.insertmanyvalues_max_parameters > 999, (
        f"insertmanyvalues_max_parameters={d.insertmanyvalues_max_parameters!r}; "
        f"the parent's pre-3.32 fallback (999) has leaked through"
    )
