"""Pin SA 2.x dialect flags so a future SA default flip is caught here, not in production."""

from __future__ import annotations

from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.sql.compiler import InsertmanyvaluesSentinelOpts

from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


def test_use_insertmanyvalues_wo_returning_pinned_false_on_sync() -> None:
    assert DqliteDialect.use_insertmanyvalues_wo_returning is False


def test_use_insertmanyvalues_wo_returning_pinned_false_on_async() -> None:
    assert DqliteDialect_aio.use_insertmanyvalues_wo_returning is False


def test_insertmanyvalues_implicit_sentinel_pinned_not_supported_on_sync() -> None:
    assert (
        DqliteDialect.insertmanyvalues_implicit_sentinel
        is InsertmanyvaluesSentinelOpts.NOT_SUPPORTED
    )


def test_insertmanyvalues_implicit_sentinel_pinned_not_supported_on_async() -> None:
    assert (
        DqliteDialect_aio.insertmanyvalues_implicit_sentinel
        is InsertmanyvaluesSentinelOpts.NOT_SUPPORTED
    )


def test_supports_for_update_of_pinned_false_on_sync() -> None:
    assert DqliteDialect.supports_for_update_of is False


def test_supports_for_update_of_pinned_false_on_async() -> None:
    assert DqliteDialect_aio.supports_for_update_of is False


def test_insert_executemany_returning_sort_by_parameter_order_pinned_true_on_sync() -> None:
    # Memoized from `insert_returning and use_insertmanyvalues`; both pinned True.
    assert DqliteDialect.insert_executemany_returning_sort_by_parameter_order is True


def test_insert_executemany_returning_sort_by_parameter_order_pinned_true_on_async() -> None:
    assert DqliteDialect_aio.insert_executemany_returning_sort_by_parameter_order is True


def test_bind_typing_pinned_none_on_sync() -> None:
    assert DqliteDialect.bind_typing is BindTyping.NONE


def test_bind_typing_pinned_none_on_async() -> None:
    assert DqliteDialect_aio.bind_typing is BindTyping.NONE


def test_is_async_false_on_sync_dialect() -> None:
    assert DqliteDialect.is_async is False


def test_is_async_true_on_async_dialect() -> None:
    assert DqliteDialect_aio.is_async is True
