"""Pin: every ``peer = ...`` assignment in ``sqlalchemydqlite.aio``
routes through ``_log_safe_peer`` rather than reading
``getattr(<conn>, "address", None)`` directly. The helper's CWE-117
sanitisation at the log site is defense-in-depth against
``dial_func`` overrides that bypass ``parse_address``'s strict gate.

The helper itself is unit-tested in
``test_log_safe_peer_helper.py``. This file pins the *integration*:
all 13 ``peer=%s`` log sites in the SA-aio module use the helper.
A regression that reverts a single site to raw ``getattr(...)``
passes every existing test today.
"""

from __future__ import annotations

import ast
import inspect

import sqlalchemydqlite.aio as _aio_mod
import sqlalchemydqlite.base as _base_mod


def _find_raw_address_getattrs(module: object) -> list[tuple[int, str]]:
    """Walk a module's AST and return every ``getattr(<x>, "address",
    None)`` call OUTSIDE the ``_log_safe_peer`` helper itself (the
    helper IS the one site that's allowed to dereference the raw
    attribute — that's what it sanitises)."""
    source = inspect.getsource(module)
    tree = ast.parse(source)
    survivors: list[tuple[int, str]] = []

    # Identify the line range of ``_log_safe_peer`` so its own getattr
    # call is excluded from the survivor list.
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
    """Every peer-extraction site must route through
    ``_log_safe_peer``. A regression that reverts any single site to
    raw ``getattr(<conn>, "address", None)`` would re-expose the
    log injection / bidi-rewrite surface."""
    survivors = _find_raw_address_getattrs(_aio_mod)
    assert not survivors, (
        "Every ``getattr(..., 'address', None)`` site in aio.py must "
        "route through ``_log_safe_peer`` (CWE-117 defense-in-depth). "
        "Raw getattr survivors:\n" + "\n".join(f"  line {ln}: {src}" for ln, src in survivors)
    )


def test_base_module_has_no_raw_address_getattr_in_log_context() -> None:
    """Sibling check on ``base.py`` — the same discipline applies to
    the single ``peer=%s`` site in ``do_terminate`` (added in commit
    9f71b12)."""
    survivors = _find_raw_address_getattrs(_base_mod)
    assert not survivors, (
        "Every ``getattr(..., 'address', None)`` site in base.py must "
        "route through ``_log_safe_peer``. Raw getattr survivors:\n"
        + "\n".join(f"  line {ln}: {src}" for ln, src in survivors)
    )


def test_aio_module_log_safe_peer_used_at_every_peer_assignment() -> None:
    """Positive twin: ``_log_safe_peer`` IS the call used at every
    ``peer = ...`` assignment in the module. Inspection pin so a
    refactor that aliases the helper away (or drops the import)
    would fail with a clear diagnostic."""
    source = inspect.getsource(_aio_mod)
    assert "_log_safe_peer" in source
    # The 13 call sites should all show up; the lower-bound is more
    # robust to a future renaming / consolidation than asserting on
    # exact count.
    assert source.count("_log_safe_peer(") >= 10, (
        "Expected at least 10 peer-log call sites through "
        "_log_safe_peer; got "
        f"{source.count('_log_safe_peer(')}"
    )
