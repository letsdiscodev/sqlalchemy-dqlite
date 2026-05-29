"""``_dqlite_run_reap_dbs`` routes peer-derived exception text through
``sanitize_for_log`` in its DEBUG record so LF cannot split it (CWE-117)."""

from __future__ import annotations

import logging
from typing import Any

import sqlalchemydqlite.provision as provision


def test_reap_dbs_debug_log_sanitises_exception_message(monkeypatch: Any, caplog: Any) -> None:
    """Forged LF in the bubbled-up exception is escaped as ``\\n`` in the record."""

    forged = "dropped\nFORGED: forged test-log entry"

    def _boom(_eng: Any) -> None:
        raise RuntimeError(forged)

    monkeypatch.setattr(provision, "_drop_user_tables", _boom)

    def _fake_create_engine(_url: Any) -> Any:
        class _E:
            def dispose(self) -> None:
                pass

        return _E()

    import sqlalchemy

    monkeypatch.setattr(sqlalchemy, "create_engine", _fake_create_engine)

    caplog.set_level(logging.DEBUG, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs("dqlite:///somewhere", ["worker_1"])

    matching = [rec for rec in caplog.records if "reap_dbs ident=" in rec.getMessage()]
    assert matching, "reap_dbs DEBUG log record not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into DEBUG log record (CWE-117): {msg!r}"
    assert "\\n" in msg, (
        f"sanitize_for_log should escape LF as the two-byte sequence backslash+n; got: {msg!r}"
    )
