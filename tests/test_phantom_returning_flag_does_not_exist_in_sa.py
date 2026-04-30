"""Pin: ``supports_sane_multi_rowcount_returning`` is NOT a real
SA 2.x attribute. We previously pinned this name as a fourth
rowcount-truthfulness flag — but SA only defines three. The
pin was a decorative no-op that would have applied to a
hypothetical future SA flag of the same name with arbitrary
semantics; the pin was removed to keep the drift-defence
surface aligned with reality.

This test guards the dropped pin: if a future SA release
DOES introduce the attribute, we'll see this fail and can
make a deliberate decision about the value rather than
inheriting a stale silent default.
"""

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
    """Pin: the REAL flag (without the ``_multi_`` infix) must
    remain pinned on our dialect. The phantom flag was the no-op
    sibling; the real one is load-bearing."""
    from sqlalchemydqlite import DqliteDialect

    assert "supports_sane_rowcount_returning" in DqliteDialect.__dict__
    assert DqliteDialect.supports_sane_rowcount_returning is False
    # Sanity: verify SA itself defines it (the pin is anchored to a
    # real upstream attribute, not a phantom).
    assert hasattr(DefaultDialect, "supports_sane_rowcount_returning")
    # Touch sqlalchemy module to keep import live in optimised builds
    # that strip unused imports.
    assert sqlalchemy is not None
