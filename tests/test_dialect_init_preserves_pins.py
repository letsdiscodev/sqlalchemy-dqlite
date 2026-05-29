"""SA's ``SQLiteDialect.__init__`` writes instance attributes that shadow our class-level pins
(on PyPy the RETURNING flags get zeroed); pin the values at instance scope."""

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
    """Parent's version-gated ``< (3, 32, 0)`` write must not leak through and cap batch
    INSERTs at 999 parameters; ``__init__`` re-applies the DefaultDialect value."""
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
    """The dbapi only accepts qmark; another paramstyle compiles SQL that fails with a cryptic
    ProgrammingError at execute time, so reject the kwarg up-front."""
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
    d = cls(paramstyle="qmark")
    assert d.paramstyle == "qmark"


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_paramstyle_kwarg_none_accepted_as_sentinel(cls: type) -> None:
    """``paramstyle=None`` is SA's "use the dbapi default" sentinel (resolves to qmark here);
    accept it rather than rejecting."""
    d = cls(paramstyle=None)
    assert d.paramstyle == "qmark"


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_broken_fk_pragma_quotes_pinned_false(cls: type) -> None:
    """SA derives this from ``sqlite_version_info < (3, 6, 14)``; pin False so lowering the
    pinned version (flipping FK-pragma quote stripping) trips. ``is False`` so a third state fails
    loudly."""
    d = cls()
    assert d._broken_fk_pragma_quotes is False


@pytest.mark.parametrize("cls", [DqliteDialect, DqliteDialect_aio])
def test_broken_dotted_colnames_pinned_false(cls: type) -> None:
    """Symmetric to ``_broken_fk_pragma_quotes``: SA derives this from
    ``sqlite_version_info < (3, 10, 0)`` (affects UNION dotted-colname stripping)."""
    d = cls()
    assert d._broken_dotted_colnames is False
