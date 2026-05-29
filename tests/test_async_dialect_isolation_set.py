"""Pin async-dialect ``set_isolation_level`` parity with the sync path:
``DqliteDialect_aio`` inherits without override and must not diverge."""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio


class TestAsyncDialectSetIsolationLevel:
    def test_none_is_rejected_with_argument_error(self) -> None:
        """SA's async pool resets isolation via the ``reset_isolation_level``
        no-op override, not ``set_isolation_level(conn, None)``, so None is
        strictly rejected here."""
        from sqlalchemy.exc import ArgumentError

        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        with pytest.raises(ArgumentError, match="non-None"):
            dialect.set_isolation_level(mock_conn, None)  # type: ignore[arg-type]
        mock_conn.cursor.assert_not_called()

    def test_serializable_is_noop(self) -> None:
        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dialect.set_isolation_level(mock_conn, "SERIALIZABLE")
        assert len(w) == 0
        mock_conn.cursor.assert_not_called()

    def test_autocommit_raises_argumenterror(self) -> None:
        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            dialect.set_isolation_level(mock_conn, "AUTOCOMMIT")

    def test_unknown_raises_argumenterror(self) -> None:
        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        with pytest.raises(ArgumentError, match="only supports SERIALIZABLE"):
            dialect.set_isolation_level(mock_conn, "READ UNCOMMITTED")
