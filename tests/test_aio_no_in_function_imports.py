"""``aio.do_ping`` and the autocommit setter must not run import
statements in their bodies: do_ping fires on every pool checkout under
``pool_pre_ping``, so a per-call import lookup is hot."""

from __future__ import annotations

import ast
import inspect
import textwrap
from typing import Any

import sqlalchemydqlite.aio as aio_mod


def _function_has_import(func: Any) -> bool:
    """True if the function's source body contains any import statement
    (recursive)."""
    src = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(src)
    return any(isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(tree))


def test_aio_do_ping_has_no_in_function_imports() -> None:
    method = aio_mod.DqliteDialect_aio.do_ping
    assert not _function_has_import(method), (
        "DqliteDialect_aio.do_ping must not contain in-function "
        "import statements; hoist them to module-top."
    )


def test_aio_autocommit_setter_has_no_in_function_imports() -> None:
    autocommit_descriptor: Any = aio_mod.AsyncAdaptedConnection.autocommit
    setter = autocommit_descriptor.fset
    assert setter is not None
    assert not _function_has_import(setter), (
        "AsyncAdaptedConnection.autocommit.setter must not contain "
        "in-function import statements; hoist them to module-top."
    )
