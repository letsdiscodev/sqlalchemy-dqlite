"""Pin: ``_dqlite_create_db`` and ``_dqlite_drop_db`` route ``ident``
through ``_safe_for_log`` so a forged ident containing U+2028 /
U+2029 / bidi controls cannot splice the operator log.

The sibling ``reap_dbs`` site already routes through the same
helper; this brings the three provision log sites into uniform
discipline.

The tests reach the inner function bodies via ``.fns["dqlite"]`` on
the SA-dispatch wrapper so no live cfg / engine is required.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.provision import _dqlite_create_db, _dqlite_drop_db


def _inner(fn: Any) -> Any:
    """Reach the dqlite-specific inner function past SA's dispatcher."""
    return fn.fns["dqlite"]


_U2028 = " "


def test_create_db_log_sanitises_ident_strips_u2028(
    caplog: pytest.LogCaptureFixture,
) -> None:
    forged = f"tenant-a{_U2028}FAKE LOG INJECTION"
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")
    _inner(_dqlite_create_db)(MagicMock(), MagicMock(), forged)

    records = [r for r in caplog.records if "create_db" in r.getMessage()]
    assert records, f"expected create_db INFO record; got {caplog.records!r}"
    assert _U2028 not in records[0].getMessage(), (
        f"U+2028 must be stripped from create_db log; got {records[0].getMessage()!r}"
    )


def test_drop_db_log_sanitises_ident_strips_u2028(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    forged = f"tenant-b{_U2028}FAKE LOG INJECTION"
    caplog.set_level(logging.INFO, logger="sqlalchemydqlite.provision")
    monkeypatch.setattr("sqlalchemydqlite.provision._drop_user_tables", lambda _eng: None)
    _inner(_dqlite_drop_db)(MagicMock(), MagicMock(), forged)

    records = [r for r in caplog.records if "drop_db" in r.getMessage()]
    assert records, f"expected drop_db INFO record; got {caplog.records!r}"
    assert _U2028 not in records[0].getMessage(), (
        f"U+2028 must be stripped from drop_db log; got {records[0].getMessage()!r}"
    )
