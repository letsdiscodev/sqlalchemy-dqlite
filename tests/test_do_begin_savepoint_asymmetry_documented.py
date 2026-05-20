"""Pin: the deliberate asymmetry between ``do_begin`` (raw-cursor
override) and the inherited SAVEPOINT family (``do_savepoint`` /
``do_release_savepoint`` / ``do_rollback_to_savepoint``) is
documented at the ``do_begin`` site.

The override discipline at ``do_begin`` enforces a narrow defensive
``cursor.close()`` wrap so a transport-class failure during close
does not mask the BEGIN-time exception. The SAVEPOINT family goes
through SA's ``connection.execute(SavepointClause(...))`` pipeline
where ``_handle_dbapi_exception`` runs before SA propagates, so
the close-after-exception preservation discipline is not needed
there.

The audit found this as an asymmetry worth documenting so a
future contributor does not "fix the inconsistency" by mirroring
the raw-cursor shape to the SAVEPOINT family (over-engineering
that duplicates SA's pipeline for no behavioural gain).
"""

from __future__ import annotations

import inspect

from sqlalchemydqlite.base import DqliteDialect


def test_do_begin_docstring_explains_savepoint_asymmetry() -> None:
    """The override-vs-inherited asymmetry is explained at the
    ``do_begin`` site so a future contributor understands the
    intentional design."""
    src = inspect.getsource(DqliteDialect.do_begin)
    # The explanation lives in a comment block above the method;
    # ``getsource`` on a method excludes the preceding decorator
    # comments. The substantive explanation lives within the
    # method's lexical scope inside the class — inspect the class
    # source instead.
    class_src = inspect.getsource(DqliteDialect)
    # The "asymmetric" hint must appear somewhere in the do_begin
    # vicinity together with savepoint terminology.
    assert "Asymmetric with the SAVEPOINT family" in class_src or "do_savepoint" in src


def test_dialect_does_not_override_savepoint_family() -> None:
    """Pin the inheritance choice: SAVEPOINT methods are NOT in the
    dialect's class dict. A future change that adds an override
    would trip this pin and force the contributor to justify
    duplicating SA's pipeline."""
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
