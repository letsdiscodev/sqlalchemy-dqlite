"""Pin: SQLAlchemy 2.x drift-defence flags on `DqliteDialect` and
`DqliteDialect_aio`.

Each flag pinned at the class level here matches a ``DefaultDialect``
default that could theoretically flip in a future SA release. Pinning
sets a maintainer-readable contract: if SA flips the default, the
test catches the divergence at our suite, not in production.

Sibling pin block: ``base.py:1212-1238`` already pins
``use_insertmanyvalues`` / ``supports_default_values`` /
``insert_null_pk_still_autoincrements``. This module covers the
SA 2.x flags added in subsequent SA releases that were not yet
pinned: ``use_insertmanyvalues_wo_returning``,
``insertmanyvalues_implicit_sentinel``, ``supports_for_update_of``,
``insert_executemany_returning_sort_by_parameter_order``,
``bind_typing``, plus the sync-side ``is_async = False`` symmetric
with the async dialect's ``is_async = True``.
"""

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
    # The flag is a memoized property on DefaultDialect derived from
    # `insert_returning and use_insertmanyvalues`. With our pins
    # (insert_returning=True, use_insertmanyvalues=True), the
    # derivation evaluates to True; pinning explicitly as a class
    # attribute makes the contract maintainer-visible.
    assert DqliteDialect.insert_executemany_returning_sort_by_parameter_order is True


def test_insert_executemany_returning_sort_by_parameter_order_pinned_true_on_async() -> None:
    assert DqliteDialect_aio.insert_executemany_returning_sort_by_parameter_order is True


def test_bind_typing_pinned_none_on_sync() -> None:
    assert DqliteDialect.bind_typing is BindTyping.NONE


def test_bind_typing_pinned_none_on_async() -> None:
    assert DqliteDialect_aio.bind_typing is BindTyping.NONE


def test_is_async_false_on_sync_dialect() -> None:
    """Sibling pin: the async dialect pins `is_async = True`; the
    sync dialect now explicitly pins `is_async = False`. Symmetric
    drift-defence."""
    assert DqliteDialect.is_async is False


def test_is_async_true_on_async_dialect() -> None:
    """Pre-existing pin at aio.py — included here so the introspection
    contract covers both sides."""
    assert DqliteDialect_aio.is_async is True
