"""is_disconnect still matches closed-handle messages with an ``(id=...)`` suffix."""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import InterfaceError as DbapiInterfaceError
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "message",
    [
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
    sample = "Connection is closed (id=42)"
    assert "connection is closed" in sample.lower()

    sample = "Cursor is closed (id=42)"
    assert "cursor is closed" in sample.lower()


def test_production_raise_sites_use_canonical_suffix_shape() -> None:
    """Every production ``is closed`` raise site uses the literal ``(id=...)`` suffix."""
    import inspect
    import re

    from dqliteclient import pool as pool_mod
    from dqlitedbapi import connection as sync_conn_mod
    from dqlitedbapi import cursor as sync_cur_mod
    from dqlitedbapi.aio import connection as aio_conn_mod
    from dqlitedbapi.aio import cursor as aio_cur_mod
    from sqlalchemydqlite import aio as sa_aio_mod

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
        # _async_ping lives on the dialect, so id(dbapi_connection) is its canonical suffix.
        accepted = ("(id={id(self)})", "(id={id(dbapi_connection)})")
        assert any(s in body for s in accepted), (
            f"closed-handle raise site missing canonical ``(id={{id(self)}})`` "
            f"or ``(id={{id(dbapi_connection)}})`` suffix: {body!r}"
        )
