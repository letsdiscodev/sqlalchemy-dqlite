"""2PC hooks raise PEP 249 ``NotSupportedError``, not ``DefaultDialect``'s
bare ``NotImplementedError``, so a standard ``except NotSupportedError``
catches the dqlite case.
"""

from __future__ import annotations

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.aio import DqliteDialect_aio
from sqlalchemydqlite.base import DqliteDialect


class TestSyncDialectTwoPhaseRaisesNotSupported:
    def test_do_begin_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_begin_twophase(connection=None, xid="x1")

    def test_do_prepare_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_prepare_twophase(connection=None, xid="x1")

    def test_do_commit_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_commit_twophase(connection=None, xid="x1")

    def test_do_rollback_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_rollback_twophase(connection=None, xid="x1")

    def test_do_recover_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_recover_twophase(connection=None)


class TestAsyncDialectInheritsTwoPhaseOverrides:
    """The async dialect inherits the base overrides; pin guards a future
    hierarchy split from losing them on the async path."""

    def test_async_dialect_do_begin_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect_aio()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_begin_twophase(connection=None, xid="x1")

    def test_async_dialect_do_prepare_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect_aio()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_prepare_twophase(connection=None, xid="x1")

    def test_async_dialect_do_commit_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect_aio()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_commit_twophase(connection=None, xid="x1")

    def test_async_dialect_do_rollback_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect_aio()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_rollback_twophase(connection=None, xid="x1")

    def test_async_dialect_do_recover_twophase_raises_not_supported(self) -> None:
        dialect = DqliteDialect_aio()
        with pytest.raises(_dbapi_exc.NotSupportedError, match="two-phase commit"):
            dialect.do_recover_twophase(connection=None)
