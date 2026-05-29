"""The adapter's bound DescriptionTuple is the dbapi alias, pinned via getattr
(not a public re-import, which would widen the adapter's public surface)."""

from __future__ import annotations

from dqlitedbapi import DescriptionTuple as _DbapiDescriptionTuple
from dqlitedbapi.types import DescriptionTuple as _DbapiTypesDescriptionTuple
from sqlalchemydqlite import aio as _adapter_module


def test_description_tuple_alias_is_shared() -> None:
    adapter_alias = _adapter_module.DescriptionTuple  # type: ignore[attr-defined]
    assert adapter_alias is _DbapiDescriptionTuple


def test_dbapi_top_level_and_types_submodule_descriptions_are_identical() -> None:
    assert _DbapiDescriptionTuple is _DbapiTypesDescriptionTuple
