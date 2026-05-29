"""Pin: SA URL pre-validation raises ArgumentError at engine construction (not first
checkout) for userinfo, fragment, and bad host shape.

Userinfo: earlier guard used ``or`` (truthy), so empty username + None password slipped
through. Fragment: SA's URL doesn't strip it, so it leaks into the trailing query value and
yields a misleading parse error. Bad host shape would otherwise surface only as a deferred
InterfaceError at first connect().
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError


class TestUserinfoStructuralRejection:
    @pytest.mark.parametrize(
        "bad_url",
        [
            "dqlite://@localhost:9001/db",
            "dqlite://:@localhost:9001/db",
            "dqlite://user@localhost:9001/db",
            "dqlite://user:pass@localhost:9001/db",
        ],
    )
    def test_userinfo_in_url_rejected(self, bad_url: str) -> None:
        with pytest.raises(ArgumentError, match="username|password"):
            create_engine(bad_url)


class TestUrlFragmentRejection:
    @pytest.mark.parametrize(
        "bad_url",
        [
            "dqlite://localhost:9001/db#frag",
            "dqlite://localhost:9001/db?timeout=5#frag",
            "dqlite://localhost:9001/db?max_total_rows=100#x",
        ],
    )
    def test_fragment_in_url_rejected(self, bad_url: str) -> None:
        with pytest.raises(ArgumentError, match="fragment"):
            create_engine(bad_url)


class TestHostShapePreValidation:
    @pytest.mark.parametrize(
        "bad_url",
        [
            "dqlite://münchen.example.com:9001/db",
            "dqlite://host1,host2:9001/db",
        ],
    )
    def test_invalid_host_shape_rejected_at_construction(self, bad_url: str) -> None:
        """Bad host shape surfaces as ArgumentError at create_engine, not deferred to connect()."""
        with pytest.raises(ArgumentError, match="host|address|hostname"):
            create_engine(bad_url)

    def test_valid_host_passes(self) -> None:
        """Ordinary hosts still construct."""
        eng = create_engine("dqlite://localhost:9001/db")
        assert eng is not None
        eng.dispose()
