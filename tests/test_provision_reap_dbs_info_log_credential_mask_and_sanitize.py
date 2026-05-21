"""``_dqlite_run_reap_dbs`` masks the URL password and sanitises the
host portion of the INFO entry-line.

The entry-line interpolates an SA URL via ``%s``. ``URL.__str__``
defaults to ``render_as_string(hide_password=False)`` — the password
becomes part of the INFO record verbatim (CWE-532). The host
portion is operator- or peer-supplied and can carry LF / U+2028 /
bidi / ZWSP that splits the log record (CWE-117). The fix calls
``parsed.render_as_string(hide_password=True)`` and wraps the result
in ``sanitize_for_log``; this test pins both halves.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.engine import URL

import sqlalchemydqlite.provision as provision


def test_reap_dbs_info_log_masks_url_password(caplog: Any) -> None:
    """Pin: a URL carrying a password renders with ``***`` (the SA
    ``hide_password=True`` mask) in the INFO record; the literal
    password MUST NOT appear."""
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
    """Pin: an LF embedded in the host portion is escaped as the
    two-byte sequence ``\\n`` (sanitize_for_log discipline), not
    survived raw."""
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
    """Regression: a credential-free URL renders the host:port
    cleanly (no ``***`` appears uselessly), preserving operator
    diagnostics."""
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
