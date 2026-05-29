"""Pin the compliance conftest's stale-fragment fail-loud check: an
empty or non-matching ``_SCHEMA_USING_PARAMETRIZE_SKIPS`` fragment raises
``UsageError`` rather than silently no-op'ing when an SA upgrade renames
parametrize axes. Loaded via ``importlib`` since the conftest brings in
SA's plugin and would hijack collection for the rest of the suite."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_CONFTEST_PATH = Path(__file__).parent / "compliance" / "conftest.py"


def _load_conftest_module() -> ModuleType:
    """Load the conftest as a plain module to call its hook directly."""
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
    """Stand-in for SA's plugin hook; we only need the snapshot logic."""
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
    """A fragment matching an item that SA's hook later deselects must
    still count, else requirement gating would falsely flag it stale."""
    mod = _load_conftest_module()

    def _sa_filter(_session: Any, _config: Any, items: list[Any]) -> None:
        items.clear()  # simulate SA requirement-gating removal

    monkeypatch.setattr(mod, "_sa_modify_items", _sa_filter)
    monkeypatch.setattr(
        mod,
        "_SCHEMA_USING_PARAMETRIZE_SKIPS",
        ("matching_fragment",),
    )
    items: list[Any] = [_FakeItem("matching_fragment_in_nodeid")]
    mod.pytest_collection_modifyitems(None, None, items)
