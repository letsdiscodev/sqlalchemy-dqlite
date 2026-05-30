"""Pin SA 2.x dialect flags so a future SA default flip is caught here, not in production."""

from __future__ import annotations

import pytest
from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.sql.compiler import InsertmanyvaluesSentinelOpts

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect

_DIALECTS = [DqliteDialect, DqliteDialect_aio]


@pytest.mark.parametrize("cls", _DIALECTS)
@pytest.mark.parametrize(
    ("attr", "expected"),
    [
        ("use_insertmanyvalues_wo_returning", False),
        ("insertmanyvalues_implicit_sentinel", InsertmanyvaluesSentinelOpts.NOT_SUPPORTED),
        ("supports_for_update_of", False),
        # Memoized from `insert_returning and use_insertmanyvalues`; both pinned True.
        ("insert_executemany_returning_sort_by_parameter_order", True),
        ("bind_typing", BindTyping.NONE),
    ],
)
def test_dialect_flag_pinned(cls: type, attr: str, expected: object) -> None:
    assert getattr(cls, attr) is expected


def test_is_async_false_on_sync_dialect() -> None:
    assert DqliteDialect.is_async is False


def test_is_async_true_on_async_dialect() -> None:
    assert DqliteDialect_aio.is_async is True
