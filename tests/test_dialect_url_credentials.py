"""dqlite has no authentication; credentials in the URL must be
rejected at parse time, not silently dropped."""

from __future__ import annotations

import pytest
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import DqliteDialect


class TestCreateConnectArgsRejectsCredentials:
    def test_username_only_rejected(self) -> None:
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="db",
            username="svc",
        )
        with pytest.raises(ArgumentError, match="username or password"):
            DqliteDialect().create_connect_args(url)

    def test_password_only_rejected(self) -> None:
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="db",
            password="not-a-real-secret",
        )
        with pytest.raises(ArgumentError, match="username or password"):
            DqliteDialect().create_connect_args(url)

    def test_both_rejected(self) -> None:
        url = URL.create(
            drivername="dqlite",
            host="localhost",
            port=9001,
            database="db",
            username="svc",
            password="not-a-real-secret",
        )
        with pytest.raises(ArgumentError, match="username or password"):
            DqliteDialect().create_connect_args(url)

    def test_no_credentials_accepted(self) -> None:
        url = URL.create(drivername="dqlite", host="localhost", port=9001, database="db")
        _, kwargs = DqliteDialect().create_connect_args(url)
        assert kwargs["address"] == "localhost:9001"
        assert kwargs["database"] == "db"
