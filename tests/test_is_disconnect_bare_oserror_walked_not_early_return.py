"""Pin: is_disconnect classifies a bare OSError (and OSError children of a
BaseExceptionGroup) via the cause-chain walk, not a top-level early return."""

from __future__ import annotations

import pytest

from sqlalchemydqlite.base import DqliteDialect


@pytest.fixture
def dialect() -> DqliteDialect:
    return DqliteDialect()


def test_bare_oserror_classified_via_walk(dialect: DqliteDialect) -> None:
    assert dialect.is_disconnect(OSError("connection refused"), None, None) is True


def test_connection_reset_error_classified(dialect: DqliteDialect) -> None:
    assert dialect.is_disconnect(ConnectionResetError("peer reset"), None, None) is True


def test_broken_pipe_error_classified(dialect: DqliteDialect) -> None:
    assert dialect.is_disconnect(BrokenPipeError("pipe broken"), None, None) is True


def test_exception_group_with_oserror_child_classified(dialect: DqliteDialect) -> None:
    """A group's OSError child classifies via walk recursion (an early return missed it)."""
    group = BaseExceptionGroup(
        "container",
        [OSError("conn refused"), RuntimeError("benign sibling")],
    )
    assert dialect.is_disconnect(group, None, None) is True


def test_nested_oserror_via_cause_chain_classified(dialect: DqliteDialect) -> None:
    try:
        try:
            raise OSError("connection refused")
        except OSError as os_err:
            raise RuntimeError("middleware re-raise") from os_err
    except RuntimeError as e:
        assert dialect.is_disconnect(e, None, None) is True


def test_non_oserror_runtimeerror_not_classified(dialect: DqliteDialect) -> None:
    """Negative pin: a RuntimeError with no OSError in the chain must not classify."""
    assert dialect.is_disconnect(RuntimeError("not a disconnect"), None, None) is False
