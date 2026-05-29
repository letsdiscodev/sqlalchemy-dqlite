"""Pin: ``supports_sane_multi_rowcount_returning`` is not a real SA attribute.
Guards the dropped no-op pin — fails if a future SA release introduces it."""

from __future__ import annotations

import sqlalchemy
from sqlalchemy.engine.default import DefaultDialect


def test_phantom_returning_flag_not_defined_in_sa_default_dialect() -> None:
    assert not hasattr(DefaultDialect, "supports_sane_multi_rowcount_returning"), (
        "SA's DefaultDialect now exposes "
        "``supports_sane_multi_rowcount_returning`` — review whether "
        "the dropped pin should be re-added with an intentional value."
    )


def test_real_returning_flag_remains_pinned() -> None:
    """Pin: the real flag (no ``_multi_`` infix) stays pinned on our dialect."""
    from sqlalchemydqlite import DqliteDialect

    assert "supports_sane_rowcount_returning" in DqliteDialect.__dict__
    assert DqliteDialect.supports_sane_rowcount_returning is False
    assert hasattr(DefaultDialect, "supports_sane_rowcount_returning")
    assert sqlalchemy is not None
