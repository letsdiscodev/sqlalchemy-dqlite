"""Pin: the dialect's ``max_total_rows=none`` warning does NOT
mention a ``=disabled`` token.

The pre-fix warning text said:
"dqlite: ``max_total_rows`` cap disabled via URL
(``?max_total_rows=none`` or ``=disabled`` token)..."

But ``_parse_url_int_or_none`` accepts ONLY the literal token
``"none"`` (case-insensitive); ``"disabled"`` is rejected with
``ArgumentError("must be a positive integer or 'none' to disable:
...")``. An operator copying the WARNING text into a runbook (set
``?max_total_rows=disabled``) hits ArgumentError at engine creation.

The fix drops the ``=disabled`` mention so the warning text matches
the parser's actual contract.
"""

from __future__ import annotations

import logging

import pytest
from sqlalchemy import exc as sa_exc

from sqlalchemydqlite.base import DqliteDialect


def test_max_total_rows_warning_does_not_mention_disabled_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Trigger the warning by passing ``max_total_rows=None`` to
    ``create_connect_args`` (the URL-parser path) and assert the
    emitted message does NOT mention a ``=disabled`` token.
    """
    DqliteDialect._max_total_rows_disabled_warning_emitted = False
    dialect = DqliteDialect()

    # Synthesize a URL whose query yields max_total_rows=None.
    from sqlalchemy.engine import URL

    url = URL.create(
        "dqlite",
        host="127.0.0.1",
        port=9999,
        database="db",
        query={"max_total_rows": "none"},
    )

    with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):
        dialect.create_connect_args(url)

    warning_records = [
        r for r in caplog.records if r.levelname == "WARNING" and "max_total_rows" in r.message
    ]
    assert warning_records, (
        f"expected max_total_rows warning; got {[r.message for r in caplog.records]}"
    )
    msg = warning_records[0].message
    assert "=disabled" not in msg, (
        f"warning must not advertise the non-existent =disabled token; got: {msg!r}"
    )
    assert "?max_total_rows=none" in msg, (
        f"warning must still advertise the documented =none token; got: {msg!r}"
    )

    # Reset the gate so other tests aren't affected.
    DqliteDialect._max_total_rows_disabled_warning_emitted = False


def test_url_parser_rejects_disabled_token() -> None:
    """Negative pin: the warning's wording must match parser
    behaviour. ``?max_total_rows=disabled`` is rejected with
    ``ArgumentError`` — so the warning's omission of ``=disabled``
    is faithful to reality.
    """
    from sqlalchemydqlite.base import _parse_url_int_or_none

    with pytest.raises(sa_exc.ArgumentError, match="must be a positive integer or 'none'"):
        _parse_url_int_or_none("max_total_rows", "disabled", upper=10_000_000)
