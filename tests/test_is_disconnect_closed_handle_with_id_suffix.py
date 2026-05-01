"""Pin: the SA ``is_disconnect`` substring classifier still returns
True for closed-handle messages enriched with an ``(id=..., ...)``
suffix.

The closed-handle exceptions across all three packages identify the
specific instance via ``(id=..., address=...)`` / ``(id=..., conn_id=...)``
suffixes so operators can answer "which connection?" from a traceback.
The substring classifier's two phrases — ``connection is closed`` and
``cursor is closed`` — MUST remain intact (verbatim, lower-case-matchable
under ``raw.lower()``) for the SA dialect to route closed-handle
errors to the disconnect-replace path. Pin both phrases against
regression in the suffix-enriched shapes.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import InterfaceError as DbapiInterfaceError
from sqlalchemydqlite.base import DqliteDialect


@pytest.mark.parametrize(
    "message",
    [
        "Connection is closed (id=140234123, address=host:9001)",
        "Connection is closed (id=140234123, address=[::1]:9001)",
        "cursor is closed (id=140234123, conn_id=140234999)",
        "Cursor is closed (id=140234123)",
    ],
)
def test_is_disconnect_matches_closed_handle_with_id_suffix(message: str) -> None:
    err = DbapiInterfaceError(message)
    assert DqliteDialect().is_disconnect(err, None, None) is True, (
        f"is_disconnect classifier must still match the closed-handle "
        f"phrase even when enriched with (id=..., ...) suffix: {message!r}"
    )


def test_is_disconnect_substring_phrases_appear_verbatim_in_shape() -> None:
    """The new shape must keep the two classifier-load-bearing
    phrases verbatim (modulo case). Pin against a future maintainer
    rewriting them into a localised / paraphrased form."""
    sample = "Connection is closed (id=42, address=host:9001)"
    assert "connection is closed" in sample.lower()

    sample = "cursor is closed (id=42)"
    assert "cursor is closed" in sample.lower()
