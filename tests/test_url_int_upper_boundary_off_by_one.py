"""Pin: SA URL int parser's upper bound is inclusive (0 < value <= upper); value == upper
is accepted and value == upper + 1 is rejected. Boundaries reference source constants so the
test tracks any re-tune."""

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
    """value == upper accepted (guards a regression flipping <= to <)."""
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
    """value == upper + 1 rejected."""
    dialect = DqliteDialect()
    url = make_url(f"dqlite://host:19001/db?{field}={upper + 1}")
    with pytest.raises(ArgumentError):
        dialect.create_connect_args(url)


def test_url_int_or_none_helper_inclusive_upper() -> None:
    assert _parse_url_int_or_none("k", "100", upper=100) == 100
    with pytest.raises(ArgumentError):
        _parse_url_int_or_none("k", "101", upper=100)


def test_max_continuation_frames_cap_is_10x_wire_default() -> None:
    """SA URL cap is exactly 10x the wire default."""
    assert _URL_MAX_CONTINUATION_FRAMES_CAP == 10 * DEFAULT_MAX_CONTINUATION_FRAMES, (
        "URL cap on max_continuation_frames must be 10x "
        "DEFAULT_MAX_CONTINUATION_FRAMES; the factor is the documented "
        "operator-tunability budget for defense-in-depth."
    )
