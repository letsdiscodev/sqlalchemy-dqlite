"""Pin: ``DqliteDialect(__init__)`` and ``DqliteDialect_aio(__init__)``
eagerly reject ``isolation_level="AUTOCOMMIT"`` at construction time
— mirroring the eager ``paramstyle != 'qmark'`` rejection in the same
``__init__``.

The downstream ``set_isolation_level`` arm raises at SA's connect-
listener step (``engine/default.py::_builtin_onconnect``) — too late.
``create_engine(..., isolation_level="AUTOCOMMIT")`` constructs cleanly
today, then surfaces ``ArgumentError`` at the first checkout with a
confusing pool-side traceback. Reject at construction so the
diagnostic points at the kwarg.
"""

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
        """SA accepts only the spaceless ``AUTOCOMMIT`` form (case-
        insensitive after ``.upper()``); underscored forms like
        ``"AUTO_COMMIT"`` are filtered out by SA itself before
        reaching ``set_isolation_level``. Match SA's narrow
        spaceless form so we don't drift narrower than SA's filter."""
        with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
            DqliteDialect(isolation_level=value)

    def test_init_accepts_isolation_level_serializable(self) -> None:
        dialect = DqliteDialect(isolation_level="SERIALIZABLE")
        # The on-connect listener key is set by SA's DefaultDialect
        # __init__ when isolation_level is a recognised value.
        assert dialect._on_connect_isolation_level == "SERIALIZABLE"

    def test_init_accepts_no_isolation_level(self) -> None:
        dialect = DqliteDialect()
        assert dialect._on_connect_isolation_level is None
