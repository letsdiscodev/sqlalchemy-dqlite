"""Pin: ``DqliteDialect.is_disconnect`` classifies a bare ``OSError``
via the cause-chain walk (depth 0). The early-return bare-``OSError``
check at the top of ``is_disconnect`` was redundant — the walk
starts at the exception itself and re-checks ``isinstance(cause,
OSError)`` per node, so deleting the early return preserves the
contract.

Also covers ``BaseExceptionGroup`` containing OSError children:
the early return failed those cases (the group isn't an OSError);
the walk recurses into group children and catches them via the
per-node arm.
"""

from __future__ import annotations

import pytest

from sqlalchemydqlite.base import DqliteDialect


@pytest.fixture
def dialect() -> DqliteDialect:
    return DqliteDialect()


def test_bare_oserror_classified_via_walk(dialect: DqliteDialect) -> None:
    """Direct OSError must classify as disconnect via the walk's
    depth-0 OSError arm."""
    assert dialect.is_disconnect(OSError("connection refused"), None, None) is True


def test_connection_reset_error_classified(dialect: DqliteDialect) -> None:
    """ConnectionResetError is an OSError subclass — must also
    classify via the walk."""
    assert dialect.is_disconnect(ConnectionResetError("peer reset"), None, None) is True


def test_broken_pipe_error_classified(dialect: DqliteDialect) -> None:
    """BrokenPipeError is an OSError subclass — must classify."""
    assert dialect.is_disconnect(BrokenPipeError("pipe broken"), None, None) is True


def test_exception_group_with_oserror_child_classified(dialect: DqliteDialect) -> None:
    """BaseExceptionGroup containing an OSError child must classify
    via the walk's recursion into group children. The early-return
    bare-isinstance check failed this case (groups aren't OSError);
    the walk's per-node arm catches it."""
    group = BaseExceptionGroup(
        "container",
        [OSError("conn refused"), RuntimeError("benign sibling")],
    )
    assert dialect.is_disconnect(group, None, None) is True


def test_nested_oserror_via_cause_chain_classified(dialect: DqliteDialect) -> None:
    """OSError wrapped as __cause__ of a RuntimeError must classify
    via the walk's cause-chain traversal."""
    try:
        try:
            raise OSError("connection refused")
        except OSError as os_err:
            raise RuntimeError("middleware re-raise") from os_err
    except RuntimeError as e:
        assert dialect.is_disconnect(e, None, None) is True


def test_non_oserror_runtimeerror_not_classified(dialect: DqliteDialect) -> None:
    """Negative pin: a bare RuntimeError with no OSError in the
    chain must NOT classify as disconnect."""
    assert dialect.is_disconnect(RuntimeError("not a disconnect"), None, None) is False
