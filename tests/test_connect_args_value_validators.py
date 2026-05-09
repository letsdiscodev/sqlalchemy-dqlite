"""Pin: ``connect_args={...}`` per-key values are validated against the
same per-key validators that the URL-query path enforces.

ISSUE-DT4 closed the unknown-key asymmetry but left the value-range
asymmetry open: ``_URL_QUERY_ALLOWED`` carries per-key validators
(e.g. ``close_timeout`` floor 0.01s) that ``_validate_connect_kwargs``
was not invoking. This test pins that the validator now runs against
``connect_args`` values too.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


def test_connect_args_close_timeout_below_floor_raises() -> None:
    """``close_timeout=0.0001`` violates the URL-query 0.01s floor."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="close_timeout"):
        dialect._validate_connect_kwargs({"close_timeout": 0.0001})


def test_connect_args_close_timeout_below_floor_carries_fin_flush_rationale() -> None:
    """The diagnostic carries the FIN-flush / TIME_WAIT explanation —
    same operator-facing surface as the dbapi-layer wrap and the
    direct ``DqliteConnection`` / ``ConnectionPool`` callers."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError) as exc:
        dialect._validate_connect_kwargs({"close_timeout": 0.0001})
    assert "FIN flushes" in str(exc.value), (
        "close_timeout floor diagnostic must include the FIN-flush "
        "rationale so SA-URL operators understand the reason for "
        "the 0.01s floor; same surface as the dbapi-layer wrap."
    )


def test_connect_args_close_timeout_at_floor_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"close_timeout": 0.01})


def test_connect_args_close_timeout_above_floor_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"close_timeout": 5.0})


def test_connect_args_timeout_zero_raises() -> None:
    """``timeout`` validator requires ``> 0`` and ``isfinite``."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="timeout"):
        dialect._validate_connect_kwargs({"timeout": 0.0})


def test_connect_args_timeout_inf_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="timeout"):
        dialect._validate_connect_kwargs({"timeout": float("inf")})


def test_connect_args_timeout_positive_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"timeout": 5.0})


def test_connect_args_unknown_key_still_raises() -> None:
    """Regression guard: the original allowlist check still fires."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="Unknown"):
        dialect._validate_connect_kwargs({"not_a_known_key": 1})


# --- Issue: connect_args bypass for converter-side bounds -----------
# Three URL-query keys carried bounds inside the converter only
# (validator=None) and were silently bypassing connect_args validation.
# Pin each bypass scenario raises ArgumentError, plus the bool-rejection
# tightening on timeout / close_timeout.


def test_connect_args_max_total_rows_above_cap_raises() -> None:
    """``max_total_rows`` URL path caps at 2**31 - 1; connect_args path
    must reject the same out-of-range int."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_total_rows"):
        dialect._validate_connect_kwargs({"max_total_rows": 10**12})


def test_connect_args_max_total_rows_zero_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_total_rows"):
        dialect._validate_connect_kwargs({"max_total_rows": 0})


def test_connect_args_max_total_rows_negative_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_total_rows"):
        dialect._validate_connect_kwargs({"max_total_rows": -1})


def test_connect_args_max_total_rows_none_accepted() -> None:
    """``None`` is the documented disable-cap sentinel."""
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"max_total_rows": None})


def test_connect_args_max_total_rows_in_range_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"max_total_rows": 1000})


def test_connect_args_max_total_rows_bool_raises() -> None:
    """``bool`` is a subclass of ``int`` in Python; ``True`` would
    otherwise pass the ``0 < v <= upper`` predicate as the integer 1.
    Reject explicitly so connect_args=={'max_total_rows': True}` is
    surfaced as a config error rather than silently capping at 1."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_total_rows"):
        dialect._validate_connect_kwargs({"max_total_rows": True})


def test_connect_args_max_continuation_frames_above_cap_raises() -> None:
    """``max_continuation_frames`` URL path caps at 1_000_000."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_continuation_frames"):
        dialect._validate_connect_kwargs({"max_continuation_frames": 50_000_000})


def test_connect_args_max_continuation_frames_zero_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_continuation_frames"):
        dialect._validate_connect_kwargs({"max_continuation_frames": 0})


def test_connect_args_max_continuation_frames_in_range_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"max_continuation_frames": 1024})


def test_connect_args_max_continuation_frames_none_accepted() -> None:
    """``None`` mirrors the URL ``?max_continuation_frames=none`` token
    that disables the defence-in-depth ceiling — keep parity."""
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"max_continuation_frames": None})


def test_connect_args_max_continuation_frames_bool_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_continuation_frames"):
        dialect._validate_connect_kwargs({"max_continuation_frames": True})


def test_connect_args_trust_server_heartbeat_string_raises() -> None:
    """URL path's strict bool parser rejects free-form strings; the
    connect_args path must too. ``"yes"`` is truthy as a Python value
    but the URL parser would have decoded it to True via the named
    token set. Bypass would let a typo like ``"flase"`` reach the
    consumer as a truthy non-empty string."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="trust_server_heartbeat"):
        dialect._validate_connect_kwargs({"trust_server_heartbeat": "yes"})


def test_connect_args_trust_server_heartbeat_int_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="trust_server_heartbeat"):
        dialect._validate_connect_kwargs({"trust_server_heartbeat": 1})


def test_connect_args_trust_server_heartbeat_true_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"trust_server_heartbeat": True})


def test_connect_args_trust_server_heartbeat_false_accepted() -> None:
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"trust_server_heartbeat": False})


def test_connect_args_timeout_bool_raises() -> None:
    """``True > 0`` and ``math.isfinite(True)`` are both True, so the
    pre-tightening validator silently accepted ``timeout=True``.
    Reject ``bool`` explicitly. URL path can never carry a bool
    (always converts a string), so this is a connect_args-only quirk."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="timeout"):
        dialect._validate_connect_kwargs({"timeout": True})


def test_connect_args_close_timeout_bool_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="close_timeout"):
        dialect._validate_connect_kwargs({"close_timeout": True})
