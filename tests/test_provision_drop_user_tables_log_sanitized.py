"""``_drop_user_tables`` sanitises peer-derived text in its DEBUG log
records.

Both DEBUG log sites in ``_drop_user_tables`` interpolate values that
can carry peer-supplied LF/CR (the bubbled-up exception text, and the
table ``name`` read from ``sqlite_master`` — SQLite's quoted-identifier
form admits LF inside a table name). Both must be routed through
``sanitize_for_log`` so journald/syslog cannot split a single record
into multiple lines (CWE-117). The sibling ``_dqlite_run_reap_dbs``
applies the same discipline.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

import sqlalchemydqlite.provision as provision


def test_drop_user_tables_per_drop_failure_sanitises_exception_and_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When the per-table DROP raises and the table ``name`` itself
    carries forged LF, both substitutions must be escaped in the DEBUG
    record.
    """
    forged_name = "evil\nFORGED: spoofed log line"
    forged_exc = "drop failed\nFORGED: spoofed exc"

    select_result = MagicMock()
    select_result.fetchall.return_value = [(forged_name,)]

    def _exec(sql: str) -> Any:
        if sql.startswith("SELECT"):
            return select_result
        raise RuntimeError(forged_exc)

    conn = MagicMock()
    conn.exec_driver_sql.side_effect = _exec
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None
    eng = MagicMock()
    eng.connect.return_value = cm

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    matching = [rec for rec in caplog.records if "on DROP TABLE" in rec.getMessage()]
    assert matching, "per-drop DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got: {msg!r}"


def test_drop_user_tables_connect_failure_sanitises_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When ``eng.connect`` raises with forged LF in its message, the
    DEBUG record must escape the LF.
    """
    forged = "connect failed\nFORGED: spoofed log line"

    eng = MagicMock()
    eng.connect.side_effect = RuntimeError(forged)

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    matching = [rec for rec in caplog.records if "during connect/exec" in rec.getMessage()]
    assert matching, "connect-failure DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got: {msg!r}"
