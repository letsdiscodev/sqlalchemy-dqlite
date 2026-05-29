"""Pin: ``__init__`` eagerly rejects ``isolation_level="AUTOCOMMIT"`` at construction so the
diagnostic points at the kwarg, rather than surfacing later at first pool checkout."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite import DqliteDialect
from sqlalchemydqlite.aio import DqliteDialect_aio


class TestDialectInitEagerAutocommitRejection:
    def test_init_rejects_isolation_level_autocommit_eagerly(self) -> None:
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect(isolation_level="AUTOCOMMIT")

    def test_init_rejects_isolation_level_autocommit_eagerly_aio(self) -> None:
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect_aio(isolation_level="AUTOCOMMIT")

    @pytest.mark.parametrize("value", ["autocommit", "AutoCommit", "AUTOCOMMIT"])
    def test_init_rejects_case_variants(self, value: str) -> None:
        # SA accepts only the spaceless AUTOCOMMIT form (case-insensitive); match that
        # narrow form so we don't drift narrower than SA's own filter.
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect(isolation_level=value)

    def test_init_accepts_isolation_level_serializable(self) -> None:
        dialect = DqliteDialect(isolation_level="SERIALIZABLE")
        assert dialect._on_connect_isolation_level == "SERIALIZABLE"

    def test_init_accepts_no_isolation_level(self) -> None:
        dialect = DqliteDialect()
        assert dialect._on_connect_isolation_level is None
