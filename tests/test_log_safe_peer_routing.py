"""Pin: every ``peer=`` log site routes through ``_log_safe_peer``, not raw
``getattr(<conn>, "address", None)`` (CWE-117 defense-in-depth)."""

from __future__ import annotations

import ast
import inspect
from typing import Any

import sqlalchemydqlite.aio as _aio_mod
import sqlalchemydqlite.base as _base_mod


def _find_raw_address_getattrs(module: Any) -> list[tuple[int, str]]:
    """Return every ``getattr(<x>, "address", None)`` call outside the
    ``_log_safe_peer`` helper (the one site allowed to read the raw attr)."""
    source = inspect.getsource(module)
    tree = ast.parse(source)
    survivors: list[tuple[int, str]] = []

    helper_lines: range = range(0, 0)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_log_safe_peer":
            end = node.end_lineno or node.lineno
            helper_lines = range(node.lineno, end + 1)
            break

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == "address"
            and node.lineno not in helper_lines
        ):
            survivors.append((node.lineno, ast.unparse(node)))
    return survivors


def test_aio_module_has_no_raw_address_getattr_in_log_context() -> None:
    survivors = _find_raw_address_getattrs(_aio_mod)
    assert not survivors, (
        "Every ``getattr(..., 'address', None)`` site in aio.py must "
        "route through ``_log_safe_peer`` (CWE-117 defense-in-depth). "
        "Raw getattr survivors:\n" + "\n".join(f"  line {ln}: {src}" for ln, src in survivors)
    )


def test_base_module_has_no_raw_address_getattr_in_log_context() -> None:
    survivors = _find_raw_address_getattrs(_base_mod)
    assert not survivors, (
        "Every ``getattr(..., 'address', None)`` site in base.py must "
        "route through ``_log_safe_peer``. Raw getattr survivors:\n"
        + "\n".join(f"  line {ln}: {src}" for ln, src in survivors)
    )


def test_aio_module_log_safe_peer_used_at_every_peer_assignment() -> None:
    source = inspect.getsource(_aio_mod)
    assert "_log_safe_peer" in source
    # Lower-bound is more robust to future rename/consolidation than exact count.
    assert source.count("_log_safe_peer(") >= 10, (
        "Expected at least 10 peer-log call sites through "
        "_log_safe_peer; got "
        f"{source.count('_log_safe_peer(')}"
    )
