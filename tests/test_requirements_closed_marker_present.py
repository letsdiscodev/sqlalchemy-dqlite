"""Pin: every deliberately-``closed()`` requirement still surfaces as
``enabled=False`` — a property rename would silently re-open the gate and
make the suite run (and fail) tests dqlite doesn't support."""

from __future__ import annotations

import pytest

from sqlalchemydqlite.requirements import Requirements


@pytest.fixture
def req() -> Requirements:
    return Requirements()


# Each name is deliberately closed in the requirements module; a rename
# trips the ``enabled=False`` assertion below.
_EXPECTED_CLOSED = (
    "two_phase_transactions",
    "parens_in_union_contained_select_w_limit_offset",
    "parens_in_union_contained_select_wo_limit_offset",
    "implicitly_named_constraints",
    "schemas",
    "cross_schema_fk_reflection",
    "regexp_match",
)


@pytest.mark.parametrize("name", _EXPECTED_CLOSED)
def test_closed_requirement_present_and_disabled(req: Requirements, name: str) -> None:
    """The property exists and surfaces as disabled."""
    assert hasattr(req, name), (
        f"Requirements.{name} expected to exist (closed() in source) "
        f"but is missing — likely a rename"
    )
    compound = getattr(req, name)
    assert not compound.enabled, (
        f"Requirements.{name} expected closed (enabled=False); "
        "got enabled=True. Either revert the change or update "
        "the expected list in this test."
    )
