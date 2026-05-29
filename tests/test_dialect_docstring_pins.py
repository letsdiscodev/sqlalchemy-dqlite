"""Docstring pins guarding against drift where a docstring describes a contract the code
does not deliver."""

from __future__ import annotations

from sqlalchemydqlite.base import DqliteDialect, _DqliteDateTime


def test_do_terminate_docstring_does_not_reference_nonexistent_self_timeout() -> None:
    text = DqliteDialect.do_terminate.__doc__ or ""
    assert "self._timeout" not in text, (
        "do_terminate docstring references DqliteDialect._timeout, "
        "but the dialect has no such attribute (the timeout lives on the "
        "dbapi Connection, not the dialect)."
    )


def test_dialect_has_no_timeout_attribute() -> None:
    dialect = DqliteDialect()
    assert not hasattr(dialect, "_timeout")


def test_on_connect_docstring_states_fk_on_by_default_with_off_recipe() -> None:
    text = DqliteDialect.on_connect.__doc__ or ""
    lower = text.lower()
    assert "foreign_keys" in lower, "on_connect docstring should mention the foreign_keys pragma."
    # Must convey ON-by-default, not the old inverted "OFF / not enabled".
    assert "on by default" in lower or "= on" in lower or "foreign_keys = on" in lower, (
        "on_connect docstring should state dqlite defaults "
        "PRAGMA foreign_keys = ON (the dialect previously claimed the "
        "opposite — FK off by default, 'pysqlite parity')."
    )
    assert "event.listens_for" in text, (
        "on_connect docstring should reference the @event.listens_for "
        "recipe pattern (here, for turning FK enforcement OFF)."
    )


def test_dqlite_datetime_docstring_mentions_mixed_writer_hazard() -> None:
    text = _DqliteDateTime.__doc__ or ""
    assert "mixed-writer" in text.lower() or "mixed writer" in text.lower(), (
        "_DqliteDateTime docstring should name the mixed-writer hazard "
        "for naive cells under DateTime(timezone=False)."
    )
