"""Verify ``CREATE TEMPORARY TABLE`` scoping is per-connection against a live
cluster. This relies on dqlite's leader-side SQLite connection being 1:1 with
the wire connection; a leader-connection-pooling refactor would break it and
silently corrupt ``temp_table_reflection`` under xdist.
"""

from __future__ import annotations

import contextlib

import pytest
from sqlalchemy import create_engine

pytestmark = pytest.mark.integration


def test_temp_table_scoping_is_per_connection(engine_url: str) -> None:
    # ``deferred`` so the two parallel connections don't serialize on the
    # writer-lock — this test is about scoping, not write contention.
    eng = create_engine(engine_url).execution_options(dqlite_session_mode="deferred")
    try:
        with eng.connect() as conn_a, eng.connect() as conn_b:
            conn_a.exec_driver_sql("CREATE TEMPORARY TABLE _dqlite_scope_check (x INT)")
            try:
                names_b = [
                    row[0]
                    for row in conn_b.exec_driver_sql(
                        "SELECT name FROM sqlite_temp_master"
                    ).fetchall()
                ]
                assert "_dqlite_scope_check" not in names_b, (
                    "TEMP TABLE leaked across connections — the "
                    "provision hook's per-connection-scoping claim is "
                    "false against this cluster; temp_table_reflection "
                    "compliance tests may silently corrupt under xdist. "
                    f"Connection B sees: {names_b!r}"
                )
                names_a = [
                    row[0]
                    for row in conn_a.exec_driver_sql(
                        "SELECT name FROM sqlite_temp_master"
                    ).fetchall()
                ]
                assert "_dqlite_scope_check" in names_a
            finally:
                # Explicit drop so the test doesn't depend on close-time auto-drop.
                with contextlib.suppress(Exception):
                    conn_a.exec_driver_sql("DROP TABLE temp._dqlite_scope_check")
    finally:
        eng.dispose()
