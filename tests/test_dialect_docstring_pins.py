"""Docstring pins for the SA dialect.

Each pin catches a class of in-tree drift where the docstring
describes a contract the code does not actually deliver.

* ``do_terminate`` previously claimed
  "``self._timeout`` (default 10 s)". The dialect owns no
  ``_timeout`` attribute; the timeout lives on the dbapi
  ``Connection``. Pin that ``self._timeout`` no longer appears
  AND that the dialect carries no ``_timeout`` attribute.
* ``on_connect`` previously claimed FK enforcement defaults OFF
  ("pysqlite parity"); dqlite-server actually defaults
  ``PRAGMA foreign_keys = ON``. Pin that the docstring states the
  ON-by-default behavior and gives the OFF recipe, so the inverted
  claim cannot creep back.
* ``_DqliteDateTime`` previously did not warn callers about the
  "naive cell under ``timezone=False`` is interpreted as UTC"
  mixed-writer hazard. Pin the docstring now mentions the hazard.
"""

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
    """Pin: the dialect intentionally does not own a ``_timeout``
    attribute."""
    dialect = DqliteDialect()
    assert not hasattr(dialect, "_timeout")


def test_on_connect_docstring_states_fk_on_by_default_with_off_recipe() -> None:
    """Pin: the on_connect override documents that dqlite defaults
    ``PRAGMA foreign_keys = ON`` (diverging from pysqlite/stdlib OFF)
    and gives the ``@event.listens_for`` recipe for turning it OFF — so
    the previously-inverted "FK off by default" claim cannot return."""
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
    """Pin: the ``_DqliteDateTime`` class docstring documents the
    naive-cell pass-through under ``timezone=False`` and the
    cross-writer-discipline hazard."""
    text = _DqliteDateTime.__doc__ or ""
    assert "mixed-writer" in text.lower() or "mixed writer" in text.lower(), (
        "_DqliteDateTime docstring should name the mixed-writer hazard "
        "for naive cells under DateTime(timezone=False)."
    )


def test_do_executemany_docstring_does_not_overpromise_drift_defence() -> None:
    """Pin: ``do_executemany`` docstring no longer overstates "drift
    defence" — it describes the actual behaviour (intentional opt-out
    from any future SA-default growth) and the bypass of any future
    SA hook dispatch."""
    text = DqliteDialect.do_executemany.__doc__ or ""
    assert "opt-out" in text.lower(), (
        "do_executemany docstring should describe the override as an "
        "intentional opt-out from future SA-default growth."
    )
    assert "super()" in text, (
        "do_executemany docstring should note that any future SA "
        "hook-dispatch is bypassed because the override does not call "
        "super()."
    )
