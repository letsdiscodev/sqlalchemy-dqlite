"""Pin: both DEBUG log sites in ``_drop_user_tables`` route peer-derived text
(exception text, ``sqlite_master`` table name) through ``sanitize_for_log``
so embedded LF/CR cannot split a log record (CWE-117)."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

import sqlalchemydqlite.provision as provision


def test_drop_user_tables_per_drop_failure_sanitises_exception_and_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Both the exception text and the table name must be LF-escaped."""
    forged_name = "evil\nFORGED: spoofed log line"
    forged_exc = "drop failed\nFORGED: spoofed exc"

    def _exec(sql: str, *args: Any) -> Any:
        if sql.startswith("SELECT name") and args:
            (params,) = args
            result = MagicMock()
            if params[0] == "table":
                result.fetchall.return_value = [(forged_name,)]
            else:
                result.fetchall.return_value = []
            return result
        if sql.startswith("SELECT count"):
            result = MagicMock()
            result.scalar.return_value = 0
            return result
        if sql.startswith("DROP"):
            raise RuntimeError(forged_exc)
        return MagicMock()

    conn = MagicMock()
    conn.exec_driver_sql.side_effect = _exec
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = None
    eng = MagicMock()
    eng.connect.return_value = cm

    with caplog.at_level(logging.DEBUG, logger="sqlalchemydqlite.provision"):
        provision._drop_user_tables(eng)

    matching = [rec for rec in caplog.records if "on DROP" in rec.getMessage()]
    assert matching, "per-drop DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got: {msg!r}"


def test_drop_user_tables_connect_failure_sanitises_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A forged LF in the connect exception message must be escaped."""
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
