"""Pin: the asymmetry between ``do_begin`` (raw-cursor override) and the
inherited SAVEPOINT family is documented at the ``do_begin`` site, so a
contributor doesn't mirror the raw-cursor shape onto SA's pipeline.
"""

from __future__ import annotations

import inspect

from sqlalchemydqlite.base import DqliteDialect


def test_do_begin_docstring_explains_savepoint_asymmetry() -> None:
    src = inspect.getsource(DqliteDialect.do_begin)
    class_src = inspect.getsource(DqliteDialect)
    assert "Asymmetric with the SAVEPOINT family" in class_src or "do_savepoint" in src


def test_dialect_does_not_override_savepoint_family() -> None:
    """SAVEPOINT methods are NOT in the dialect's class dict (inherited)."""
    for name in (
        "do_savepoint",
        "do_release_savepoint",
        "do_rollback_to_savepoint",
    ):
        assert name not in DqliteDialect.__dict__, (
            f"{name} is inherited from DefaultDialect (routes through "
            f"connection.execute(SavepointClause)). A local override "
            f"would duplicate SA's pipeline; see the do_begin docstring "
            f"for the rationale."
        )
