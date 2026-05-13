"""Pin the description-tuple alias to the dbapi-layer single source
of truth. If a future refactor re-declares ``DescriptionTuple``
locally it must not drift from the dbapi shape; importing ensures
drift can only happen by deliberate coordinated change to both
modules.

The adapter consumes ``DescriptionTuple`` from
``dqlitedbapi.types`` purely for type annotations on its
``_Description`` alias; nothing in the adapter re-exports the name.
This test pins module-attribute identity by inspecting the
adapter's bound symbol via ``getattr`` rather than asserting it via
a public re-import (which would force the adapter to widen its
public surface unnecessarily).
"""

from __future__ import annotations

from dqlitedbapi import DescriptionTuple as _DbapiDescriptionTuple
from dqlitedbapi.types import DescriptionTuple as _DbapiTypesDescriptionTuple
from sqlalchemydqlite import aio as _adapter_module


def test_description_tuple_alias_is_shared() -> None:
    """The adapter's bound ``DescriptionTuple`` IS the dbapi alias."""
    adapter_alias = _adapter_module.DescriptionTuple  # type: ignore[attr-defined]
    assert adapter_alias is _DbapiDescriptionTuple


def test_dbapi_top_level_and_types_submodule_descriptions_are_identical() -> None:
    """The curated top-level re-export resolves to the same object as
    the private ``dqlitedbapi.types`` source."""
    assert _DbapiDescriptionTuple is _DbapiTypesDescriptionTuple
