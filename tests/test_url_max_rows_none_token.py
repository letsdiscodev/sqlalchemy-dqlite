"""Pin: ``?max_total_rows=none`` (and ``?max_continuation_frames=none``)
disable the cap, mirroring the dbapi ``connect(max_total_rows=None)``
capability so URL-driven config (twelve-factor, env-var-driven engines)
can express the same intent.

Without the literal ``"none"`` token, operators configuring SA via DSN
strings have no way to disable the row/frame cap — a regression for
the URL-vs-Python API parity.
"""

from __future__ import annotations

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


class TestMaxTotalRowsNoneToken:
    def test_none_token_lowercase(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=none")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] is None

    def test_none_token_uppercase(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=NONE")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] is None

    def test_none_token_mixed_case(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=None")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] is None

    def test_positive_integer_still_accepted(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=5000")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] == 5000

    def test_zero_still_rejected(self) -> None:
        """Negative pin: zero is still out-of-range — only the literal
        ``"none"`` disables. ``0`` would be ambiguous (no rows allowed?
        unbounded?)."""
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=0")
        with pytest.raises(ArgumentError, match="out of range"):
            dialect.create_connect_args(url)

    def test_negative_still_rejected(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=-1")
        with pytest.raises(ArgumentError, match="out of range"):
            dialect.create_connect_args(url)

    def test_garbage_token_rejected(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=infinite")
        with pytest.raises(ArgumentError):
            dialect.create_connect_args(url)


class TestMaxContinuationFramesNoneToken:
    def test_none_token(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_continuation_frames=none")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_continuation_frames"] is None

    def test_positive_integer_still_accepted(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_continuation_frames=500")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_continuation_frames"] == 500

    def test_above_dialect_ceiling_rejected(self) -> None:
        """The dialect's defense-in-depth 1_000_000 frame ceiling is
        still enforced for integer values."""
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_continuation_frames=99999999")
        with pytest.raises(ArgumentError, match="out of range"):
            dialect.create_connect_args(url)


class TestNoneTokenPlumbsToDbapi:
    def test_both_none_pass_through(self) -> None:
        dialect = DqliteDialect()
        url = make_url("dqlite://host:19001/db?max_total_rows=none&max_continuation_frames=none")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["max_total_rows"] is None
        assert kwargs["max_continuation_frames"] is None
