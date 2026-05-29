"""``connect_args={...}`` per-key values run the same validators as the
URL-query path (e.g. ``close_timeout`` 0.01s floor), so connect_args
can't bypass them."""

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
    """The diagnostic carries the FIN-flush rationale, same as the
    dbapi-layer wrap."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError) as exc:
        dialect._validate_connect_kwargs({"close_timeout": 0.0001})
    assert "FIN flushes" in str(exc.value)


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


# connect_args isolation_level=AUTOCOMMIT must surface the dedicated
# _AUTOCOMMIT_REJECTION_MSG, matching the engine-level rejection.


def test_connect_args_isolation_level_autocommit_dedicated_message() -> None:
    from sqlalchemydqlite.base import _AUTOCOMMIT_REJECTION_MSG

    dialect = DqliteDialect()
    with pytest.raises(ArgumentError) as exc:
        dialect._validate_connect_kwargs({"isolation_level": "AUTOCOMMIT"})
    assert _AUTOCOMMIT_REJECTION_MSG in str(exc.value)


def test_connect_args_isolation_level_autocommit_case_insensitive() -> None:
    """The compare is case-insensitive, matching engine-level."""
    from sqlalchemydqlite.base import _AUTOCOMMIT_REJECTION_MSG

    dialect = DqliteDialect()
    for spelling in ("autocommit", "AutoCommit", "AUTOCOMMIT"):
        with pytest.raises(ArgumentError) as exc:
            dialect._validate_connect_kwargs({"isolation_level": spelling})
        assert _AUTOCOMMIT_REJECTION_MSG in str(exc.value)


def test_connect_args_isolation_level_other_value_directs_to_engine_level() -> None:
    """A non-AUTOCOMMIT ``isolation_level`` in connect_args gets a message
    pointing at the engine-level kwarg, not the generic "Unknown" one."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="create_engine"):
        dialect._validate_connect_kwargs({"isolation_level": "SERIALIZABLE"})


def test_connect_args_isolation_level_none_directs_to_engine_level() -> None:
    """``None`` (the stdlib autocommit shape) also gets the directional
    engine-level message."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="create_engine"):
        dialect._validate_connect_kwargs({"isolation_level": None})


# Three URL-query keys carried bounds in the converter only (validator=None)
# and bypassed connect_args validation; pin each, plus bool rejection on
# timeout/close_timeout.


def test_connect_args_max_total_rows_above_cap_raises() -> None:
    """``max_total_rows`` URL path caps at 2**31 - 1; connect_args too."""
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
    """``bool`` subclasses ``int``; ``True`` would pass as 1, so reject
    it explicitly rather than silently capping at 1."""
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
    """``None`` disables the ceiling, mirroring the URL ``=none`` token."""
    dialect = DqliteDialect()
    dialect._validate_connect_kwargs({"max_continuation_frames": None})


def test_connect_args_max_continuation_frames_bool_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="max_continuation_frames"):
        dialect._validate_connect_kwargs({"max_continuation_frames": True})


def test_connect_args_trust_server_heartbeat_string_raises() -> None:
    """Strict bool: reject free-form strings on connect_args too, else a
    typo like ``"flase"`` reaches the consumer as a truthy string."""
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
    """``True > 0`` and ``isfinite(True)`` both pass, so reject ``bool``
    explicitly (connect_args-only; URL always carries a string)."""
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="timeout"):
        dialect._validate_connect_kwargs({"timeout": True})


def test_connect_args_close_timeout_bool_raises() -> None:
    dialect = DqliteDialect()
    with pytest.raises(ArgumentError, match="close_timeout"):
        dialect._validate_connect_kwargs({"close_timeout": True})
