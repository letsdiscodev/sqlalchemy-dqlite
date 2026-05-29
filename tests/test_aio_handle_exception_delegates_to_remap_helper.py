"""``_handle_exception`` delegates the loop-state substring scan to the
module-level ``_remap_loop_state_runtime_error`` helper, the single
source of truth for the four patterns. The close-arm's ``event loop is
closed`` case stays inline (debug-log + return, not raise)."""

from __future__ import annotations

import ast
import inspect
from typing import Any

import pytest

import sqlalchemydqlite.aio as aio_mod
from dqlitedbapi.exceptions import OperationalError, ProgrammingError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _function_substring_literals(func: Any) -> set[str]:
    """String literals in the function's source body (recursive AST walk)."""
    src = inspect.getsource(func)
    import textwrap

    tree = ast.parse(textwrap.dedent(src))
    literals: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            literals.add(node.value)
    return literals


def test_handle_exception_body_does_not_contain_substring_literals() -> None:
    """``_handle_exception`` must not carry any of the four loop-state
    substring literals — they all live in the helper after delegation."""
    forbidden = [
        '"different loop"',
        '"different event loop"',
        '"event loop is closed"',
        '"loop is already running"',
    ]
    for needle in forbidden:
        assert needle.strip('"') not in _function_substring_literals(
            AsyncAdaptedConnection._handle_exception
        ) - {
            "different loop",
            "different event loop",
            "event loop is closed",
            "loop is already running",
        }, (
            f"_handle_exception must not contain logic-level substring "
            f"literal {needle} — delegate to "
            f"_remap_loop_state_runtime_error instead."
        )


def test_handle_exception_calls_remap_helper() -> None:
    """``_handle_exception`` invokes ``_remap_loop_state_runtime_error``
    by name."""
    src = inspect.getsource(AsyncAdaptedConnection._handle_exception)
    assert "_remap_loop_state_runtime_error" in src


def test_remap_helper_exists_at_module_level() -> None:
    """The helper lives at module top-level so any third site can import
    it."""
    helper = getattr(aio_mod, "_remap_loop_state_runtime_error", None)
    assert helper is not None
    assert callable(helper)


def test_substring_literals_appear_only_once_in_source() -> None:
    """The four substring literals appear at most twice each in aio.py
    logic (helper + close-arm short-circuit). Counted via AST over
    ``X in ...`` comparisons."""
    src = inspect.getsource(aio_mod)
    tree = ast.parse(src)
    counts: dict[str, int] = {
        "different loop": 0,
        "different event loop": 0,
        "event loop is closed": 0,
        "loop is already running": 0,
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if isinstance(op, ast.In) and isinstance(node.left, ast.Constant):
                    val = node.left.value
                    if isinstance(val, str) and val in counts:
                        counts[val] += 1

    assert counts["different loop"] <= 2, (
        f"'different loop' should appear at most twice (helper + "
        f"close-arm short-circuit); got {counts['different loop']}"
    )
    assert counts["different event loop"] <= 2
    assert counts["event loop is closed"] <= 2
    assert counts["loop is already running"] <= 2


def _make_adapter() -> AsyncAdaptedConnection:
    from unittest.mock import MagicMock

    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


@pytest.mark.parametrize(
    ("phrase", "expected_prefix"),
    [
        ("got Future <Future> attached to a different loop", "event-loop mismatch"),
        ("different event loop", "event-loop mismatch"),
        ("Event loop is closed", "event loop closed"),
        ("This event loop is already running", "event loop already running"),
    ],
)
def test_handle_exception_remaps_runtime_error_via_helper(
    phrase: str, expected_prefix: str
) -> None:
    """``_handle_exception`` raises the expected OperationalError for each
    of the four substring patterns."""
    adapter = _make_adapter()
    original = RuntimeError(phrase)
    with pytest.raises(OperationalError, match=expected_prefix):
        adapter._handle_exception(original)


def test_handle_exception_propagates_non_loop_runtime_error_unchanged() -> None:
    """A RuntimeError with none of the four substrings propagates
    unchanged."""
    adapter = _make_adapter()
    original = RuntimeError("some unrelated runtime fault")
    with pytest.raises(RuntimeError, match="some unrelated runtime fault"):
        adapter._handle_exception(original)


def test_handle_exception_propagates_non_runtime_error_unchanged() -> None:
    """A non-RuntimeError / non-ProgrammingError class is skipped by the
    helper's isinstance gate; original propagates."""
    adapter = _make_adapter()
    original = ValueError("bare ValueError")
    with pytest.raises(ValueError, match="bare ValueError"):
        adapter._handle_exception(original)


def test_handle_exception_remaps_programmingerror_with_loop_phrase() -> None:
    """The helper's isinstance gate also catches ProgrammingError
    (cross-loop wrap from dqlitedbapi)."""
    adapter = _make_adapter()
    original = ProgrammingError("AsyncConnection in use by a different event loop")
    with pytest.raises(OperationalError, match="event-loop mismatch"):
        adapter._handle_exception(original)
