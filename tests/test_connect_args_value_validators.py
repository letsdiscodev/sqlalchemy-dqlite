"""Pin: ``connect_args={...}`` per-key values are validated against the
same per-key validators that the URL-query path enforces.

The unknown-key check rejects keys not in ``_URL_QUERY_ALLOWED``;
the per-key value validators (e.g. ``close_timeout`` floor 0.01s)
must run on both the URL-query path AND the ``connect_args`` path.
This test pins the ``connect_args`` value-range arm so a regression
that bypasses the validators for ``connect_args`` is caught.
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


# --- isolation_level=AUTOCOMMIT diagnostic parity ------------------
# The engine-level rejection at base.py:1035-1037 surfaces the
# dedicated _AUTOCOMMIT_REJECTION_MSG when create_engine(...,
# isolation_level="AUTOCOMMIT") is used. The connect_args= overlay path
# previously fell through to the generic "Unknown dqlite connect kwarg"
# message — same root cause, less helpful diagnostic. Pin parity so
# both paths surface the dedicated message.


def test_connect_args_isolation_level_autocommit_dedicated_message() -> None:
    from sqlalchemydqlite.base import _AUTOCOMMIT_REJECTION_MSG

    dialect = DqliteDialect()
    with pytest.raises(ArgumentError) as exc:
        dialect._validate_connect_kwargs({"isolation_level": "AUTOCOMMIT"})
    assert _AUTOCOMMIT_REJECTION_MSG in str(exc.value)


def test_connect_args_isolation_level_autocommit_case_insensitive() -> None:
    """Engine-level uses ``iso_level.upper() == "AUTOCOMMIT"``; the
    connect_args path must mirror the same case-insensitive compare."""
    from sqlalchemydqlite.base import _AUTOCOMMIT_REJECTION_MSG

    dialect = DqliteDialect()
    for spelling in ("autocommit", "AutoCommit", "AUTOCOMMIT"):
        with pytest.raises(ArgumentError) as exc:
            dialect._validate_connect_kwargs({"isolation_level": spelling})
        assert _AUTOCOMMIT_REJECTION_MSG in str(exc.value), (
            f"connect_args isolation_level={spelling!r} must surface "
            f"the dedicated AUTOCOMMIT diagnostic, matching engine-level."
        )


def test_connect_args_isolation_level_other_value_falls_through_to_generic() -> None:
    """Only AUTOCOMMIT is special-cased; other ``isolation_level``
    values are still unknown kwargs and fall through to the generic
    allowlist rejection. Don't widen the allowlist."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="Unknown"):
        dialect._validate_connect_kwargs({"isolation_level": "SERIALIZABLE"})


def test_connect_args_isolation_level_none_falls_through_to_generic() -> None:
    """Engine-level guard requires ``isinstance(iso_level, str)``;
    mirror that — ``None`` is not a string, so no special case fires
    and the generic allowlist rejection runs."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="Unknown"):
        dialect._validate_connect_kwargs({"isolation_level": None})


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
