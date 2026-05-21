"""Docstring pins for the SA dialect.

Each pin catches a class of in-tree drift where the docstring
describes a contract the code does not actually deliver.

* ``do_terminate`` previously claimed
  "``self._timeout`` (default 10 s)". The dialect owns no
  ``_timeout`` attribute; the timeout lives on the dbapi
  ``Connection``. Pin that ``self._timeout`` no longer appears
  AND that the dialect carries no ``_timeout`` attribute.
* ``on_connect`` previously omitted any reference to FK enforcement.
  Pin the rewrite includes the FK-PRAGMA recipe so a contributor
  reading the docstring is directed to the correct extension point.
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


def test_on_connect_docstring_mentions_fk_pragma_recipe() -> None:
    """Pin: the on_connect override documents the FK-PRAGMA recipe so
    a contributor or operator reading the docstring is directed to
    the correct extension point."""
    text = DqliteDialect.on_connect.__doc__ or ""
    assert "foreign_keys" in text.lower(), (
        "on_connect docstring should mention the FK-PRAGMA recipe so "
        "operators know the dialect does not enable foreign-key "
        "enforcement by default."
    )
    assert "event.listens_for" in text, (
        "on_connect docstring should reference the @event.listens_for "
        "recipe pattern for applications that need FK enforcement."
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
