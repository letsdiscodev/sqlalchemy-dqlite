"""Pin: ``Requirements`` opens the SQLite-supported SuiteRequirements that
default to ``closed()`` upstream, so the suite doesn't skip supported features."""

from __future__ import annotations

from sqlalchemy.testing.exclusions import compound

from sqlalchemydqlite.requirements import Requirements


def _is_open(prop: compound) -> bool:
    """True for ``open()``: ``fails`` is empty, vs non-empty for ``closed()``.

    The public ``enabled_for_config`` needs a config object we lack here, so
    inspect the internal ``fails`` attribute instead.
    """
    return len(prop.fails) == 0


def test_nullsordering_is_open() -> None:
    assert _is_open(Requirements().nullsordering)


def test_intersect_is_open() -> None:
    assert _is_open(Requirements().intersect)


def test_except_is_open() -> None:
    assert _is_open(Requirements().except_)


def test_index_ddl_if_exists_is_open() -> None:
    assert _is_open(Requirements().index_ddl_if_exists)
