"""Pin: ``_DqliteTime.bind_processor`` returns ``None`` (opt-out).

PEP 249 / SA's TypeEngine treats a ``None`` return from
``bind_processor`` as the documented opt-out: the dialect's bind
machinery short-circuits the per-value callable. Returning
``lambda v: v`` would look functionally equivalent but disable that
short-circuit AND interpose a TypeEngine boundary on every bound
value. The result side has 8+ tests; the bind side had zero — this
pins the intentional pass-through against an "obvious-looking"
regression.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

from sqlalchemydqlite.base import _DqliteTime


def test_bind_processor_returns_none_for_default_dialect() -> None:
    dialect = MagicMock()
    # ``bind_processor`` is annotated ``-> None`` (the documented
    # opt-out) so mypy refuses to bind the return; cast to ``Any``
    # and assert the runtime contract that drives SA's bind-side
    # short-circuit.
    proc = cast(Any, _DqliteTime().bind_processor(dialect))
    assert proc is None


def test_bind_processor_returns_none_for_timezone_true_variant() -> None:
    """Timezone-true ``Time`` also defers to the parent's bind side."""
    dialect = MagicMock()
    proc = cast(Any, _DqliteTime(timezone=True).bind_processor(dialect))
    assert proc is None
