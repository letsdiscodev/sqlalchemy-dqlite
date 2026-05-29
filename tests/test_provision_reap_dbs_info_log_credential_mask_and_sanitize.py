"""``_dqlite_run_reap_dbs`` masks the URL password (CWE-532) and sanitises the
peer-supplied host portion (CWE-117) in its INFO entry-line."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.engine import URL

import sqlalchemydqlite.provision as provision


def test_reap_dbs_info_log_masks_url_password(caplog: Any) -> None:
    """A password renders as ``***``; the literal password must not appear."""
    url = URL.create(
        drivername="dqlite+aio",
        username="admin",
        password="s3cret",
        host="cluster.example.com",
        port=9001,
        database="main",
    )
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs(url, [])

    matching = [r for r in caplog.records if "dqlite reap_dbs:" in r.getMessage()]
    assert matching, "reap_dbs INFO entry-line not emitted"
    msg = matching[0].getMessage()
    assert "s3cret" not in msg, f"password leaked into INFO log: {msg!r}"
    assert "***" in msg, f"hide_password mask missing from log: {msg!r}"


def test_reap_dbs_info_log_escapes_lf_in_host(caplog: Any) -> None:
    """An LF in the host is escaped as ``\\n``, not survived raw."""
    malformed = URL.create(
        drivername="dqlite",
        host="evil.example.com\n[CRITICAL] forged",
        port=9001,
        database="main",
    )
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs(malformed, [])

    matching = [r for r in caplog.records if "dqlite reap_dbs:" in r.getMessage()]
    assert matching, "reap_dbs INFO entry-line not emitted"
    msg = matching[0].getMessage()
    assert "\n" not in msg, f"raw LF leaked into INFO log (CWE-117): {msg!r}"
    assert "\\n" in msg, f"sanitize_for_log should escape LF as backslash+n; got {msg!r}"


def test_reap_dbs_info_log_credential_free_url_renders_cleanly(caplog: Any) -> None:
    """A credential-free URL renders host:port cleanly with no spurious ``***``."""
    url = URL.create(
        drivername="dqlite",
        host="cluster.example.com",
        port=9001,
        database="main",
    )
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")

    provision._dqlite_run_reap_dbs(url, [])

    matching = [r for r in caplog.records if "dqlite reap_dbs:" in r.getMessage()]
    assert matching, "reap_dbs INFO entry-line not emitted"
    msg = matching[0].getMessage()
    assert "cluster.example.com" in msg
    assert "9001" in msg
    assert "***" not in msg, "hide_password mask should not appear absent a password"
