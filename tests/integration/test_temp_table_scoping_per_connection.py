"""Integration: verify ``CREATE TEMPORARY TABLE`` scoping is
per-connection against a live dqlite cluster.

The provision hook ``_dqlite_temp_table_keyword_args`` and the
``temp_table_reflection`` requirement both assert TEMP TABLE
semantics apply per-connection. dqlite's leader-only write model
routes every CREATE TEMPORARY TABLE through Raft to the leader's
SQLite library; per-wire-connection scoping requires the leader-
side SQLite connection mapping to be 1:1 with the wire connection.
Skim of dqlite-upstream gateway.c shows each ``struct gateway``
carries its own leader SQLite connection — so the claim IS true
today, but a future leader-connection-pooling refactor would
silently break ``temp_table_reflection`` compliance tests under
xdist (one worker's temp table would leak into another worker's
``sqlite_temp_master`` reflection).

Pin the scoping with a live cross-connection test so a refactor
that breaks the 1:1 mapping fails loudly here rather than
silently corrupting xdist runs.
"""

from __future__ import annotations

import contextlib

import pytest
from sqlalchemy import create_engine

pytestmark = pytest.mark.integration


def test_temp_table_scoping_is_per_connection(engine_url: str) -> None:
    # Use ``dqlite_session_mode="deferred"`` so the two parallel
    # connections each opening an implicit tx don't serialize at
    # the writer-lock — this test exercises temp-table scoping,
    # not write contention.
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
                # TEMP tables auto-drop on connection close; be
                # explicit so the test does not depend on close
                # timing.
                with contextlib.suppress(Exception):
                    conn_a.exec_driver_sql("DROP TABLE temp._dqlite_scope_check")
    finally:
        eng.dispose()
