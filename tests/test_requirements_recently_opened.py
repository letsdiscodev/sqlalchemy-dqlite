"""Pin: ``Requirements`` opens the SQLite-supported SuiteRequirements
that default to ``closed()`` upstream.

Each property below is a SQL feature SQLite (and therefore dqlite)
supports natively. Without these explicit ``exclusions.open()``
overrides, the SA compliance suite skips every test gated on
them — silent coverage gaps on demonstrably-supported features.

If a future SA upgrade adds a new fence to one of these
requirements, the corresponding compliance test will fail and the
override should be re-evaluated against the new fence.
"""

from __future__ import annotations

from sqlalchemy.testing.exclusions import compound

from sqlalchemydqlite.requirements import Requirements


def _is_open(prop: compound) -> bool:
    """An ``exclusions.open()`` instance has no enabled exclusion
    rules; ``closed()`` has at least one. The compound's
    ``__bool__`` returns True if it would skip — so an open
    compound is falsy."""
    # SA's `compound.enabled_for_config(config)` is the public
    # check, but we don't have a config object here. Instead,
    # inspect the internal `fails` attribute which is empty for
    # open() and non-empty for closed().
    return len(prop.fails) == 0


def test_nullsordering_is_open() -> None:
    assert _is_open(Requirements().nullsordering)


def test_intersect_is_open() -> None:
    assert _is_open(Requirements().intersect)


def test_except_is_open() -> None:
    assert _is_open(Requirements().except_)


def test_index_ddl_if_exists_is_open() -> None:
    assert _is_open(Requirements().index_ddl_if_exists)
