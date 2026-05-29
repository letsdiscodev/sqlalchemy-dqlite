"""Parity pin: each first-party disconnect substring in SA's ``_dqlite_disconnect_messages`` must
still appear in a producer source, since there's no SSOT and a rename silently disables
``is_disconnect`` classification. SA-only (CPython/asyncio) substrings are excluded."""

from __future__ import annotations

import ast
import pathlib
from types import ModuleType

import dqliteclient.cluster
import dqliteclient.connection
import dqliteclient.protocol
import dqlitedbapi.aio.connection
import dqlitedbapi.aio.cursor
import dqlitedbapi.connection
import dqlitedbapi.cursor
import sqlalchemydqlite.base
from sqlalchemydqlite.base import DqliteDialect


def _emitted_string_literals(mod: ModuleType) -> set[str]:
    """Lower-cased string literals in ``raise``/``Call`` args (incl. f-string parts); comments
    and docstrings excluded, so a stale comment can't mask a producer rename."""
    file_attr = mod.__file__
    assert file_attr is not None, f"{mod!r} has no __file__"
    tree = ast.parse(pathlib.Path(file_attr).read_text())
    literals: set[str] = set()

    def _collect_from(parent: ast.AST) -> None:
        for sub in ast.walk(parent):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                literals.add(sub.value.lower())
            elif isinstance(sub, ast.JoinedStr):
                for part in sub.values:
                    if isinstance(part, ast.Constant) and isinstance(part.value, str):
                        literals.add(part.value.lower())

    for node in ast.walk(tree):
        if isinstance(node, ast.Raise) and node.exc is not None:
            _collect_from(node.exc)
        elif isinstance(node, ast.Call):
            for arg in node.args:
                _collect_from(arg)
            for kw in node.keywords:
                if kw.value is not None:
                    _collect_from(kw.value)
    return literals


def _consumer_substring_literals(mod: ModuleType) -> set[str]:
    """Lower-cased string literals in ``Compare`` nodes (e.g. ``"foo" in message``), to catch
    disconnect substrings recognised via inline ``is_disconnect`` arms rather than the tuple."""
    file_attr = mod.__file__
    assert file_attr is not None, f"{mod!r} has no __file__"
    tree = ast.parse(pathlib.Path(file_attr).read_text())
    literals: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            for operand in (node.left, *node.comparators):
                if isinstance(operand, ast.Constant) and isinstance(operand.value, str):
                    literals.add(operand.value.lower())
    return literals


# Lowercased first-party substrings -> producer modules; the substring must appear in at
# least one listed module (a rename trips the test). CPython/asyncio substrings excluded.
_FIRST_PARTY_SUBSTRINGS: dict[str, list[ModuleType]] = {
    "connection closed": [dqliteclient.protocol],
    "failed to connect": [
        dqliteclient.cluster,
        dqliteclient.connection,
        dqlitedbapi.connection,
    ],
    "not connected": [dqliteclient.connection],
    # Matched via inline is_disconnect arm to route the cancel-after-invalidate signal to
    # pool invalidation.
    "connection invalidated (id=": [
        dqlitedbapi.connection,
        dqlitedbapi.aio.connection,
    ],
    # Matched via inline is_disconnect Compare arm, not the bulk tuple.
    "connection is closed": [
        dqlitedbapi.connection,
        dqlitedbapi.aio.connection,
    ],
    "cursor is closed": [
        dqlitedbapi.cursor,
        dqlitedbapi.aio.cursor,
    ],
}


def test_sa_first_party_disconnect_substrings_emitted_by_producers() -> None:
    """Each first-party disconnect substring appears in a producer's ``Raise``/``Call`` literal
    (so a rename trips, but a stale comment doesn't)."""
    sa_tuple_substrings = DqliteDialect._dqlite_disconnect_messages
    # Also accept literals from inline is_disconnect Compare arms, not just the bulk tuple.
    sa_compare_literals = _consumer_substring_literals(sqlalchemydqlite.base)
    for substring, producer_mods in _FIRST_PARTY_SUBSTRINGS.items():
        in_tuple = substring in sa_tuple_substrings
        in_compare = any(substring in lit for lit in sa_compare_literals)
        assert in_tuple or in_compare, (
            f"SA dialect lost {substring!r} — either restore the entry "
            f"in _dqlite_disconnect_messages, the inline is_disconnect "
            f"arm, or update this test."
        )
        emitted: set[str] = set()
        for mod in producer_mods:
            emitted |= _emitted_string_literals(mod)
        assert any(substring in lit for lit in emitted), (
            f"SA expects substring {substring!r} from one of "
            f"{[m.__name__ for m in producer_mods]} but no Raise / "
            f"Call literal contains it; comments and docstrings are "
            f"NOT scanned. Restore the producer wording or update "
            f"the SA classifier."
        )


def test_sa_wire_decode_failed_prefix_substring_identity_with_wire() -> None:
    """``WIRE_DECODE_FAILED_PREFIX`` IS the wire-layer constant; pinned to detect SA copy drift."""
    from dqlitewire import WIRE_DECODE_FAILED_PREFIX as wire_prefix

    sa_substrings = DqliteDialect._dqlite_disconnect_messages
    assert wire_prefix in sa_substrings
