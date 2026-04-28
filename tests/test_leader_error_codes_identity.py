"""Pin: the SA dialect imports the same ``LEADER_ERROR_CODES``
constant the wire layer publishes — no local override or rename
silently diverges leader-change classification."""

from __future__ import annotations


def test_dialect_uses_wire_leader_error_codes_identity() -> None:
    import sqlalchemydqlite.base as _base
    from dqlitewire import LEADER_ERROR_CODES as wire_codes

    # Direct attribute access; mypy ignore reflects the
    # re-import-without-export pattern (the constant lives in
    # base.py via ``from dqlitewire import ...`` but isn't in
    # base.py's ``__all__``).
    assert _base.LEADER_ERROR_CODES is wire_codes  # type: ignore[attr-defined]
