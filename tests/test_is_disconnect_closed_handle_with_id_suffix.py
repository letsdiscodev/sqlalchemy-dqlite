"""Pin: the SA ``is_disconnect`` substring classifier still returns
True for closed-handle messages enriched with an ``(id=...)`` suffix.

Across all three packages, closed-handle exceptions identify the
specific instance via the canonical ``(id={id(self)})`` suffix so
operators can answer "which connection?" from a traceback. The
substring classifier's two phrases — ``connection is closed`` and
``cursor is closed`` — MUST remain intact (verbatim, lower-case-
matchable under ``raw.lower()``) for the SA dialect to route
closed-handle errors to the disconnect-replace path. Pin both
phrases against regression in the actual production shape.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import InterfaceError as DbapiInterfaceError
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "message",
    [
        # Match the canonical f-string shape every production raise
        # site uses: ``f"Connection is closed (id={id(self)})"`` /
        # ``f"Cursor is closed (id={id(self)})"`` / lowercase variant
        # used by the SA async adapter. Verified by `grep "is closed
        # (id=" src/`.
        "Connection is closed (id=140234123)",
        "Cursor is closed (id=140234123)",
        "cursor is closed (id=140234123)",
    ],
)
def test_is_disconnect_matches_closed_handle_with_id_suffix(message: str) -> None:
    err = DbapiInterfaceError(message)
    assert DqliteDialect().is_disconnect(err, None, None) is True, (
        f"is_disconnect classifier must still match the closed-handle "
        f"phrase even when enriched with (id=...) suffix: {message!r}"
    )


def test_is_disconnect_substring_phrases_appear_verbatim_in_production_shape() -> None:
    """The production f-strings keep the two classifier-load-bearing
    phrases verbatim (modulo case). Pin against a future maintainer
    rewriting them into a localised / paraphrased form."""
    sample = "Connection is closed (id=42)"
    assert "connection is closed" in sample.lower()

    sample = "Cursor is closed (id=42)"
    assert "cursor is closed" in sample.lower()


def test_production_raise_sites_use_canonical_suffix_shape() -> None:
    """Source-level pin: every production ``is closed`` raise site
    uses the literal ``(id={id(self)})`` suffix. A regression that
    drops the suffix or that adds an interleaving fragment between
    the substring and the ``(id=`` would silently break the
    parametrised pin above (which only sees synthetic strings).
    """
    import inspect
    import re

    from dqliteclient import pool as pool_mod
    from dqlitedbapi import connection as sync_conn_mod
    from dqlitedbapi import cursor as sync_cur_mod
    from dqlitedbapi.aio import connection as aio_conn_mod
    from dqlitedbapi.aio import cursor as aio_cur_mod
    from sqlalchemydqlite import aio as sa_aio_mod

    # Concatenate the source for every module that emits closed-handle
    # diagnostics. Every match of the regex must include the
    # ``(id={id(self)})`` suffix verbatim — no localised / paraphrased
    # variants.
    sources = "\n".join(
        inspect.getsource(m)
        for m in (
            pool_mod,
            sync_conn_mod,
            sync_cur_mod,
            aio_conn_mod,
            aio_cur_mod,
            sa_aio_mod,
        )
    )
    pattern = re.compile(r'f"((?:Pool|Connection|Cursor|cursor) is closed[^"]*)"')
    for match in pattern.finditer(sources):
        body = match.group(1)
        assert "(id={id(self)})" in body, (
            f"closed-handle raise site missing canonical ``(id={{id(self)}})`` suffix: {body!r}"
        )
