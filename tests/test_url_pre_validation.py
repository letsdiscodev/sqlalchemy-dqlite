"""Pin: SA URL pre-validation surfaces structural / host-shape errors
at engine-construction time, not at first checkout.

Three structural URL features dqlite does not accept; each must
raise ``ArgumentError`` from ``create_connect_args``:

1. Userinfo of any shape (bare `@`, empty username, empty password,
   or filled credentials). The earlier guard used ``or`` (truthy
   check), so empty-string username + None password silently
   passed.
2. Fragment (URI ``#frag`` portion). SA's ``URL`` does not strip it,
   so the fragment leaks into the trailing query value and produces
   a misleading parse error pointing at the value, not at the misplaced
   ``#``.
3. Invalid host shape (IDN, comma-separated, etc.). The dbapi layer
   already rejects these via ``InterfaceError``, but the failure
   surfaces only at first ``engine.connect()``. Pre-validate at the
   SA layer so engine construction fails immediately with a clear
   ``ArgumentError``.
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
        """Host shape that the client rejects must surface as
        ArgumentError from create_engine, not as a deferred
        InterfaceError from first engine.connect()."""
        with pytest.raises(ArgumentError, match="host|address|hostname"):
            create_engine(bad_url)

    def test_valid_host_passes(self) -> None:
        """Sanity: ordinary hosts still construct."""
        eng = create_engine("dqlite://localhost:9001/db")
        assert eng is not None
        eng.dispose()
