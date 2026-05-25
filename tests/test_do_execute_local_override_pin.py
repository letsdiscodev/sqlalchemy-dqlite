"""Pin: ``DqliteDialect.do_execute`` and ``do_execute_no_params`` are
defined locally for drift defence, mirroring the
``do_executemany`` opt-out.

SA's three default execute hooks at ``engine/default.py:948-955`` are
identical-shaped one-liners that share the same drift surface (a
future dispatch event, an envelope tracer, a paramstyle conversion).
Pinning one but inheriting the other two means a future SA release
that grows wrapper logic at ``do_execute`` / ``do_execute_no_params``
silently lands on dqlite while the parallel change to
``do_executemany`` is opt-ed out — exactly the failure mode the
``do_executemany`` override docstring describes.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemydqlite.base import DqliteDialect


def test_do_execute_is_overridden_locally() -> None:
    assert "do_execute" in DqliteDialect.__dict__, (
        "DqliteDialect.do_execute must be pinned locally for drift "
        "defence — see do_executemany's docstring for the rationale."
    )


def test_do_execute_no_params_is_overridden_locally() -> None:
    assert "do_execute_no_params" in DqliteDialect.__dict__, (
        "DqliteDialect.do_execute_no_params must be pinned locally for "
        "drift defence — see do_executemany's docstring for the rationale."
    )


def test_do_execute_body_is_byte_equivalent_to_sa_default() -> None:
    """Body must match SA's one-line pass-through. Drive a stub cursor
    and assert it sees one call with the verbatim arguments."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()

    statement = "INSERT INTO t VALUES (?)"
    parameters = (1,)

    dialect.do_execute(cursor, statement, parameters)

    cursor.execute.assert_called_once_with(statement, parameters)


def test_do_execute_accepts_optional_context_kwarg() -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()
    dialect.do_execute(cursor, "UPDATE t SET v=? WHERE k=?", (10, 1), context=MagicMock())
    cursor.execute.assert_called_once_with("UPDATE t SET v=? WHERE k=?", (10, 1))


def test_do_execute_no_params_body_is_byte_equivalent_to_sa_default() -> None:
    """Body must match SA's one-line pass-through. The signature has
    no ``parameters`` argument; the cursor must see a single-arg call."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()

    statement = "VACUUM"

    dialect.do_execute_no_params(cursor, statement)

    cursor.execute.assert_called_once_with(statement)


def test_do_execute_no_params_accepts_optional_context_kwarg() -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    cursor = MagicMock()
    dialect.do_execute_no_params(cursor, "ANALYZE", context=MagicMock())
    cursor.execute.assert_called_once_with("ANALYZE")
