"""The SA dialect imports the wire layer's LEADER_ERROR_CODES (no local
override or rename that would diverge leader-change classification)."""

from __future__ import annotations


def test_dialect_uses_wire_leader_error_codes_identity() -> None:
    import sqlalchemydqlite.base as _base
    from dqlitewire import LEADER_ERROR_CODES as wire_codes

    # base.py re-imports the constant but omits it from __all__, hence the ignore.
    assert _base.LEADER_ERROR_CODES is wire_codes  # type: ignore[attr-defined]
