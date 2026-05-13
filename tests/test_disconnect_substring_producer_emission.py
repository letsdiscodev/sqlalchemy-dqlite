"""Cross-package producer-emission parity for SA's disconnect substring scan.

The SA dialect's ``_dqlite_disconnect_messages`` tuple (in
``sqlalchemydqlite.base``) carries case-insensitive substrings the
``is_disconnect`` classifier matches against to recognise transport-
class failures emitted by the lower layers (client and dbapi). The
substrings are bare literals; the producer sites in client / dbapi
emit matching literals. There is no SSOT between the two — a producer
rename silently disables the SA classification and the pool retains
broken slots.

A full SSOT lift would migrate all 7 substrings to the wire layer and
have producers/consumers import them. That is a larger refactor with
its own scope concerns. This file is the minimum-viable parity pin:
inspect the producer module sources and assert each first-party
disconnect substring appears verbatim somewhere in the producer
source. A rename on the producer side ripples to a test failure here,
giving the maintainer a chance to either update both sides together
or DELETE the substring from SA's scan if no longer needed.

SA-only substrings (matched against Python ``RuntimeError`` text, not
first-party emission) are EXCLUDED from this check — they originate
from CPython / asyncio.
"""

from __future__ import annotations

import pathlib
from types import ModuleType

import dqliteclient.cluster
import dqliteclient.connection
import dqliteclient.protocol
import dqlitedbapi.connection
from sqlalchemydqlite.base import DqliteDialect


def _read(mod: ModuleType) -> str:
    """Read the source of a module."""
    file_attr = mod.__file__
    assert file_attr is not None, f"{mod!r} has no __file__"
    return pathlib.Path(file_attr).read_text()


# Substrings the SA dialect matches against, that originate from
# first-party producer sites (NOT from CPython / asyncio internals).
# Keys are the lowercased substring as it appears in the SA tuple;
# values list the producer modules whose source must contain the
# substring (case-insensitive, since the SA scan lowercases input).
# Each entry requires the substring to appear in at least one of the
# listed producer modules — a rename trips the test.
_FIRST_PARTY_SUBSTRINGS: dict[str, list[ModuleType]] = {
    "connection closed": [dqliteclient.protocol],
    "failed to connect": [
        dqliteclient.cluster,
        dqliteclient.connection,
        dqlitedbapi.connection,
    ],
    "not connected": [dqliteclient.connection],
}


def test_sa_first_party_disconnect_substrings_emitted_by_producers() -> None:
    """Each first-party SA disconnect substring appears in at least
    one producer module's source. A producer rename trips this test."""
    sa_substrings = DqliteDialect._dqlite_disconnect_messages
    for substring, producer_mods in _FIRST_PARTY_SUBSTRINGS.items():
        assert substring in sa_substrings, (
            f"SA dialect lost {substring!r} — either restore the entry "
            f"in _dqlite_disconnect_messages or update this test."
        )
        sources = [_read(mod).lower() for mod in producer_mods]
        assert any(substring in src for src in sources), (
            f"SA expects substring {substring!r} from one of "
            f"{[m.__name__ for m in producer_mods]} but no producer source "
            f"contains a matching literal. Either update the SA "
            f"classifier or restore the producer wording."
        )


def test_sa_wire_decode_failed_prefix_substring_identity_with_wire() -> None:
    """``WIRE_DECODE_FAILED_PREFIX`` IS the wire-layer constant —
    pinned to detect any local copy drift in SA."""
    from dqlitewire import WIRE_DECODE_FAILED_PREFIX as wire_prefix

    sa_substrings = DqliteDialect._dqlite_disconnect_messages
    assert wire_prefix in sa_substrings
