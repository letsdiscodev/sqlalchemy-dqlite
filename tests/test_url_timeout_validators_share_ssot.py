"""Pin: all three URL timeout validators (close/dial/attempt) delegate to the client-layer
validate_timeout SSOT, so a future client-side tightening can't be bypassed by the
hand-rolled inline lambdas dial/attempt used before this fix."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import (
    _validate_attempt_timeout_url,
    _validate_close_timeout_url,
    _validate_dial_timeout_url,
)


@pytest.mark.parametrize(
    "validator,field_name",
    [
        (_validate_close_timeout_url, "close_timeout"),
        (_validate_dial_timeout_url, "dial_timeout"),
        (_validate_attempt_timeout_url, "attempt_timeout"),
    ],
)
def test_validator_rejects_bool(validator: object, field_name: str) -> None:
    """All three reject bool uniformly."""
    with pytest.raises(ArgumentError):
        validator(True)  # type: ignore[operator]
    with pytest.raises(ArgumentError):
        validator(False)  # type: ignore[operator]
    del field_name


@pytest.mark.parametrize(
    "validator",
    [_validate_close_timeout_url, _validate_dial_timeout_url, _validate_attempt_timeout_url],
)
def test_validator_rejects_negative(validator: object) -> None:
    """All three reject negative values uniformly."""
    with pytest.raises(ArgumentError):
        validator(-1.0)  # type: ignore[operator]


@pytest.mark.parametrize(
    "validator",
    [_validate_close_timeout_url, _validate_dial_timeout_url, _validate_attempt_timeout_url],
)
def test_validator_rejects_inf(validator: object) -> None:
    """All three reject inf uniformly."""
    with pytest.raises(ArgumentError):
        validator(float("inf"))  # type: ignore[operator]


@pytest.mark.parametrize(
    "validator",
    [_validate_close_timeout_url, _validate_dial_timeout_url, _validate_attempt_timeout_url],
)
def test_validator_rejects_nan(validator: object) -> None:
    """All three reject NaN uniformly."""
    with pytest.raises(ArgumentError):
        validator(float("nan"))  # type: ignore[operator]


def test_dial_timeout_field_name_in_message() -> None:
    """The rejection diagnostic names the offending field."""
    with pytest.raises(ArgumentError, match="dial_timeout"):
        _validate_dial_timeout_url(-1.0)


def test_attempt_timeout_field_name_in_message() -> None:
    with pytest.raises(ArgumentError, match="attempt_timeout"):
        _validate_attempt_timeout_url(-1.0)


def test_close_timeout_floor_rationale_preserved() -> None:
    """close_timeout's FIN-flush floor rationale stays in the URL-layer rejection."""
    with pytest.raises(ArgumentError) as excinfo:
        _validate_close_timeout_url(0.0001)
    assert "FIN" in str(excinfo.value) or "0.01" in str(excinfo.value)
