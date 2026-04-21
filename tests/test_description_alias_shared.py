"""Pin the description-tuple alias to the dbapi-layer single source
of truth. If a future refactor re-declares ``_DescriptionTuple``
locally it must not drift from the dbapi shape; importing ensures
drift can only happen by deliberate coordinated change to both
modules.
"""

from __future__ import annotations

from dqlitedbapi.types import _DescriptionTuple as _DbapiDescriptionTuple
from sqlalchemydqlite.aio import _DescriptionTuple as _AdapterDescriptionTuple


def test_description_tuple_alias_is_shared() -> None:
    """The adapter imports the dbapi-layer alias — ``is`` must hold."""
    assert _AdapterDescriptionTuple is _DbapiDescriptionTuple
