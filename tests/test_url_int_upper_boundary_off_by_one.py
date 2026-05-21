"""Pin: SA URL int parser's upper-bound predicate is inclusive
(``0 < value <= upper``), so ``value == upper`` MUST be accepted and
``value == upper + 1`` MUST be rejected.

Existing tests cover the ``value == upper`` accept and the
``value == 0`` / ``value == -1`` rejections, but the
``value == upper + 1`` case is untested — a classic off-by-one gap.
A loosening regression (``<= upper + 1``) would silently broaden
acceptance beyond the documented cap.

The upper for ``max_continuation_frames`` is derived from
``_URL_MAX_CONTINUATION_FRAMES_CAP`` (= 10 ×
``DEFAULT_MAX_CONTINUATION_FRAMES``); the upper for
``max_total_rows`` is the uint32 protocol invariant ``2**31 - 1``.
Pin the boundary via the source constants rather than literal numbers
so the test stays in lockstep with any future re-tune.
"""

from __future__ import annotations

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from dqlitewire import DEFAULT_MAX_CONTINUATION_FRAMES
from sqlalchemydqlite.base import (
    _URL_MAX_CONTINUATION_FRAMES_CAP,
    DqliteDialect,
    _parse_url_int_or_none,
)

# Match the production caps in base.py:_URL_QUERY_ALLOWED entries.
_MAX_TOTAL_ROWS_UPPER: int = 2**31 - 1
_MAX_CONTINUATION_FRAMES_UPPER: int = _URL_MAX_CONTINUATION_FRAMES_CAP


@pytest.mark.parametrize(
    ("field", "upper"),
    [
        ("max_total_rows", _MAX_TOTAL_ROWS_UPPER),
        ("max_continuation_frames", _MAX_CONTINUATION_FRAMES_UPPER),
    ],
)
def test_url_int_inclusive_upper_boundary_accepted(field: str, upper: int) -> None:
    """``value == upper`` MUST be accepted (predicate is ``<=``, not
    ``<``). Defense against a regression that flips to ``<``,
    silently rejecting the canonical maximum."""
    dialect = DqliteDialect()
    url = make_url(f"dqlite://host:19001/db?{field}={upper}")
    _, kwargs = dialect.create_connect_args(url)
    assert kwargs[field] == upper


@pytest.mark.parametrize(
    ("field", "upper"),
    [
        ("max_total_rows", _MAX_TOTAL_ROWS_UPPER),
        ("max_continuation_frames", _MAX_CONTINUATION_FRAMES_UPPER),
    ],
)
def test_url_int_upper_plus_one_rejected(field: str, upper: int) -> None:
    """``value == upper + 1`` MUST be rejected. Distinguishes
    ``<= upper`` from ``< upper + 1`` (which would loosen the cap
    by 1)."""
    dialect = DqliteDialect()
    url = make_url(f"dqlite://host:19001/db?{field}={upper + 1}")
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_url_int_or_none_helper_inclusive_upper() -> None:
    """Direct unit-level pin on the helper, fast-feedback for the
    boundary contract."""
    assert _parse_url_int_or_none("k", "100", upper=100) == 100
    with pytest.raises(ArgumentError):
        _parse_url_int_or_none("k", "101", upper=100)


def test_max_continuation_frames_cap_is_10x_wire_default() -> None:
    """Pin the derivation: the SA URL cap is exactly 10× the wire
    default. A future contributor changing the factor in either
    direction must update this assertion."""
    assert _URL_MAX_CONTINUATION_FRAMES_CAP == 10 * DEFAULT_MAX_CONTINUATION_FRAMES, (
        "URL cap on max_continuation_frames must be 10x "
        "DEFAULT_MAX_CONTINUATION_FRAMES; the factor is the documented "
        "operator-tunability budget for defense-in-depth."
    )
