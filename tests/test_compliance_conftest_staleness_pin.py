"""Pin the compliance conftest's stale-fragment fail-loud check.

The compliance ``conftest.py`` hooks ``pytest_collection_modifyitems``
to apply a hand-maintained skip list (``_SCHEMA_USING_PARAMETRIZE_SKIPS``).
SA's parametrize-id rendering is a private contract, so a future SA
upgrade that renames or recombines axes can leave a fragment matching
zero collected tests — silently no-op'ing the skip and surfacing the
unexpected test execution as a confusing pass / fail later.

Pin three behaviours:

1. An empty fragment raises ``pytest.UsageError`` (would match every
   nodeid).
2. A stale fragment (matches no collected test) raises ``pytest.UsageError``.
3. A fragment that DOES match at least one nodeid is accepted (the
   matching test gets the skip marker).

The conftest module is loaded only when running ``pytest tests/compliance/``
(it brings in SA's plugin which would replace pytest's own collection
for the rest of the suite). To unit-test the hook, import the module
file directly via ``importlib`` so this regular-tests run loads the
file as a plain Python module.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_CONFTEST_PATH = Path(__file__).parent / "compliance" / "conftest.py"


def _load_conftest_module() -> ModuleType:
    """Load the compliance conftest as a regular module so we can call
    ``pytest_collection_modifyitems`` directly with synthetic items.
    """
    spec = importlib.util.spec_from_file_location("_compliance_conftest_under_test", _CONFTEST_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _FakeItem:
    def __init__(self, nodeid: str) -> None:
        self.nodeid = nodeid
        self.markers: list[Any] = []

    def add_marker(self, marker: Any) -> None:
        self.markers.append(marker)


def _no_op_sa_modify(*_args: Any, **_kwargs: Any) -> None:
    """Stand-in for SA's plugin hook when invoking the conftest's
    hook directly. The real hook does requirement-based exclusion;
    for the unit test we don't need that, just the pre/post snapshot
    logic."""
    return None


def test_stale_fragment_raises_usage_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_conftest_module()
    monkeypatch.setattr(mod, "_sa_modify_items", _no_op_sa_modify)
    monkeypatch.setattr(
        mod,
        "_SCHEMA_USING_PARAMETRIZE_SKIPS",
        ("this_fragment_will_not_match_anything",),
    )
    items = [_FakeItem("tests/compliance/test_real.py::test_something")]
    with pytest.raises(pytest.UsageError, match="matched no collected SA test"):
        mod.pytest_collection_modifyitems(None, None, items)


def test_empty_fragment_raises_usage_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_conftest_module()
    monkeypatch.setattr(mod, "_sa_modify_items", _no_op_sa_modify)
    monkeypatch.setattr(mod, "_SCHEMA_USING_PARAMETRIZE_SKIPS", ("",))
    items = [_FakeItem("tests/compliance/test_real.py::test_something")]
    with pytest.raises(pytest.UsageError, match="Empty fragment"):
        mod.pytest_collection_modifyitems(None, None, items)


def test_matching_fragment_marks_item_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_conftest_module()
    monkeypatch.setattr(mod, "_sa_modify_items", _no_op_sa_modify)
    monkeypatch.setattr(
        mod,
        "_SCHEMA_USING_PARAMETRIZE_SKIPS",
        ("test_metadata[True-_exclusions_00-True]",),
    )
    matching = _FakeItem(
        "tests/compliance/test_reflection.py::ComponentReflectionTest::"
        "test_metadata[True-_exclusions_00-True]"
    )
    other = _FakeItem("tests/compliance/test_other.py::test_unrelated")
    items: list[Any] = [matching, other]
    mod.pytest_collection_modifyitems(None, None, items)
    assert matching.markers, "fragment matched the nodeid; the conftest must add a skip marker"
    assert not other.markers, "non-matching items must not get the skip marker"


def test_pre_delegation_snapshot_survives_sa_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If SA's hook deselects items (removing them from the list), the
    pre-snapshot must still credit any fragment that matched a removed
    item. Otherwise SA's requirement gating would falsely flag our
    fragment as stale."""
    mod = _load_conftest_module()

    def _sa_filter(_session: Any, _config: Any, items: list[Any]) -> None:
        # Simulate SA removing the matching item via requirement gating.
        items.clear()

    monkeypatch.setattr(mod, "_sa_modify_items", _sa_filter)
    monkeypatch.setattr(
        mod,
        "_SCHEMA_USING_PARAMETRIZE_SKIPS",
        ("matching_fragment",),
    )
    items: list[Any] = [_FakeItem("matching_fragment_in_nodeid")]
    # No UsageError: pre-snapshot saw the match before SA filtered.
    mod.pytest_collection_modifyitems(None, None, items)
