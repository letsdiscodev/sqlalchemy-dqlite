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


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_paramstyle_kwarg_override_raises_argument_error(cls: type) -> None:
    """``DefaultDialect.__init__`` accepts a ``paramstyle`` kwarg and
    overwrites the class-level ``"qmark"`` pin. The dbapi only accepts
    qmark; silently accepting another paramstyle would compile SQL
    that produces cryptic ProgrammingError at execute time. Reject
    the kwarg up-front."""
    from sqlalchemy.exc import ArgumentError

    with pytest.raises(ArgumentError, match="paramstyle"):
        cls(paramstyle="named")
    with pytest.raises(ArgumentError, match="paramstyle"):
        cls(paramstyle="format")
    with pytest.raises(ArgumentError, match="paramstyle"):
        cls(paramstyle="pyformat")
    with pytest.raises(ArgumentError, match="paramstyle"):
        cls(paramstyle="numeric")


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_paramstyle_kwarg_qmark_accepted_as_noop(cls: type) -> None:
    """Negative pin: explicit ``paramstyle="qmark"`` (matching the
    pin) is accepted as a no-op."""
    d = cls(paramstyle="qmark")
    assert d.paramstyle == "qmark"


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_paramstyle_kwarg_none_accepted_as_sentinel(cls: type) -> None:
    """``DefaultDialect.__init__(paramstyle=None, ...)`` is the
    documented SA "use the dbapi default" sentinel; ``None`` resolves
    via ``self.dbapi.paramstyle`` to ``"qmark"`` in our case. The
    dialect must accept this legitimate sentinel rather than rejecting
    with ArgumentError. Mirror SA's own DefaultDialect contract."""
    d = cls(paramstyle=None)
    assert d.paramstyle == "qmark"


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_broken_fk_pragma_quotes_pinned_false(cls: type) -> None:
    """SA's ``SQLiteDialect.__init__`` derives this flag from
    ``self.dbapi.sqlite_version_info < (3, 6, 14)``. The dqlitedbapi
    pinned ``SQLITE_VERSION_INFO`` is the sole input; a future
    regression that lowered the pinned tuple would silently flip the
    flag True and change ``get_foreign_keys`` reflection behaviour
    (FK-pragma quote stripping). Pin False as drift defence — if this
    test fires, triage whether the cause is (1) SA changed the gate
    threshold, (2) we lowered ``SQLITE_VERSION_INFO``, or (3)
    intentional. Use ``is False`` rather than ``is not True`` so a
    future SA flip to a third state breaks the pin loudly rather
    than silently passing."""
    d = cls()
    assert d._broken_fk_pragma_quotes is False


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_broken_dotted_colnames_pinned_false(cls: type) -> None:
    """Symmetric pin to ``_broken_fk_pragma_quotes`` — SA's
    ``SQLiteDialect.__init__`` derives this from
    ``sqlite_version_info < (3, 10, 0)`` and it influences UNION
    dotted-colname stripping during result processing. Drift defence
    same as the sibling pin above."""
    d = cls()
    assert d._broken_dotted_colnames is False
