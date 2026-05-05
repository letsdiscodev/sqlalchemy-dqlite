"""Pin: every requirement we deliberately mark ``exclusions.closed()``
must still surface as ``enabled=False`` to the SA compliance suite.

The compliance suite gates each test on
``Requirements.<name>.enabled``. A typo in the property name silently
re-opens the gate — the suite then runs the test, which fails with
cluster-side ``OperationalError: unknown database test_schema`` (or
similar) rather than skip-side "dialect closes schemas". This pin
fences the gate integrity so a refactor that renames a property
fails loudly at the unit-test level.
"""

from __future__ import annotations

import pytest

from sqlalchemydqlite.requirements import Requirements


@pytest.fixture
def req() -> Requirements:
    return Requirements()


# Each name here is documented as deliberately closed in the requirements
# module. If the property is renamed without updating this list, the
# accompanying ``enabled=False`` assertion will surface the rename.
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
    """The property exists AND surfaces as disabled. A rename or a
    silent change from ``exclusions.closed()`` to ``open()`` /
    ``only_on(...)`` re-enables the SA compliance test that depends
    on it; this pin fails loudly when that happens."""
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
