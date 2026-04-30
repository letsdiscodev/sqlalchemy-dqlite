"""Pin async-dialect ``set_isolation_level`` parity with the sync path.

The sync ``DqliteDialect.set_isolation_level`` is well-pinned by
``test_dialect.py::TestIsolationLevel``. The async ``DqliteDialect_aio``
inherits that method without override; SA's async pool resets isolation
between checkouts via ``set_isolation_level(conn, None)`` on the async
dialect class, so a future async-only override that diverges from the
sync semantics would break async pool checkouts silently.

Mirror the four sync test cases on the async dialect class:
- ``None`` → no-op, no cursor/attribute access on the dbapi conn.
- ``"SERIALIZABLE"`` → no-op, no warning.
- ``"AUTOCOMMIT"`` → ArgumentError with the dqlite-specific message.
- unknown (``"READ UNCOMMITTED"``) → ArgumentError ("only supports SERIALIZABLE").
"""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.aio import DqliteDialect_aio


class TestAsyncDialectSetIsolationLevel:
    def test_none_is_noop_no_connection_access(self) -> None:
        """SA's async pool resets isolation via ``set_isolation_level(conn, None)``.
        Pin the true no-op contract on the async dialect: no cursor, no
        attribute access, no warning. A future async-only override that
        routed ``None`` through autocommit setup would break SA's reset path.
        """
        dialect = DqliteDialect_aio()
        mock_conn = MagicMock()
        dialect.set_isolation_level(mock_conn, None)
        mock_conn.cursor.assert_not_called()
        assert mock_conn.mock_calls == []

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
