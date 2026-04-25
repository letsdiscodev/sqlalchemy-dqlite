"""Two-phase commit hooks raise PEP 249 ``NotSupportedError``.

requirements.py declares ``two_phase_transactions = exclusions.closed()``
so the SA compliance suite skips 2PC tests. At runtime, however, a
caller invoking ``connection.begin_twophase(xid)`` reaches the
dialect's ``do_begin_twophase`` (and friends). Without explicit
overrides, SA's ``DefaultDialect`` raises bare ``NotImplementedError``
— a Python-language signal, not the PEP 249 ``NotSupportedError`` a
DBAPI consumer expects when "feature unavailable in this backend."

Pin the override: each of the four 2PC hooks (plus
``do_prepare_twophase``) raises ``NotSupportedError`` with a clear
message. Callers writing the standard ``except NotSupportedError``
catch the dqlite case correctly without needing a duplicate
``except NotImplementedError`` branch.

The async dialect (``DqliteDialect_aio``) inherits from
``DqliteDialect``, so a single override on the base class covers
both sync and async surfaces.
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
    """The async dialect inherits the base overrides; pinning ensures
    a future refactor that splits the hierarchy doesn't lose the
    overrides on the async path."""

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
