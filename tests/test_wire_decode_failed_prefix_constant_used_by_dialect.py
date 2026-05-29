"""``_dqlite_disconnect_messages`` references the ``WIRE_DECODE_FAILED_PREFIX`` constant
verbatim (not a literal copy), so a rename stays in lockstep and grep-discoverable."""

from __future__ import annotations

from dqlitewire import WIRE_DECODE_FAILED_PREFIX
from sqlalchemydqlite.base import DqliteDialect


def test_dialect_disconnect_substrings_contain_wire_decode_failed_prefix() -> None:
    assert WIRE_DECODE_FAILED_PREFIX in DqliteDialect._dqlite_disconnect_messages, (
        "DqliteDialect._dqlite_disconnect_messages must contain the canonical "
        "WIRE_DECODE_FAILED_PREFIX constant from dqlitewire so the "
        "wire-decode disconnect classification stays in lockstep on rename"
    )
