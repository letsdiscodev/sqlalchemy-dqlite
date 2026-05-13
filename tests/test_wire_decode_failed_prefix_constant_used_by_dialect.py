"""Pin: SA's ``_dqlite_disconnect_messages`` substring tuple
contains the shared ``WIRE_DECODE_FAILED_PREFIX`` constant verbatim
(not a hardcoded copy of the literal).

A refactor that renames the constant must update the substring
tuple too. With the literal embedded, that link was implicit;
referencing the constant makes it explicit and grep-discoverable.
"""

from __future__ import annotations

from dqlitewire import WIRE_DECODE_FAILED_PREFIX
from sqlalchemydqlite.base import DqliteDialect


def test_dialect_disconnect_substrings_contain_wire_decode_failed_prefix() -> None:
    """The constant from dqlitewire is one of the substrings SA's
    ``is_disconnect`` scans for after lowercasing the rendered
    exception text. The presence-pin ensures the constant is the
    source of truth, not a co-incidental literal match."""
    assert WIRE_DECODE_FAILED_PREFIX in DqliteDialect._dqlite_disconnect_messages, (
        "DqliteDialect._dqlite_disconnect_messages must contain the canonical "
        "WIRE_DECODE_FAILED_PREFIX constant from dqlitewire so the "
        "wire-decode disconnect classification stays in lockstep on rename"
    )
