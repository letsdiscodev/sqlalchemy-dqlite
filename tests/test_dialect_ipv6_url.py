"""Pin: the dqlite dialect re-introduces brackets around IPv6 hosts
when building the ``address`` kwarg for the dbapi.

SQLAlchemy's URL parser strips brackets from IPv6 literals — a URL
like ``dqlite://[::1]:9001/db`` surfaces as ``url.host == "::1"``,
``url.port == 9001``. The dqlite-client address parser
(``_parse_address``) requires brackets for any host containing
colons, otherwise it cannot tell ``"::1:9001"`` from a
five-segment IPv6 with no port. So the dialect must re-bracket
multi-colon hosts before passing the address to the dbapi.

IPv4 and DNS hostnames are untouched (no colons, no ambiguity).
"""

from __future__ import annotations

import pytest
from sqlalchemy.engine.url import URL

from dqliteclient.connection import _parse_address
from sqlalchemydqlite.base import DqliteDialect


def _connect_kwargs(host: str, port: int = 9001) -> dict[str, object]:
    url = URL.create("dqlite", host=host, port=port, database="test")
    _, kwargs = DqliteDialect().create_connect_args(url)
    return kwargs


def test_ipv6_loopback_address_is_bracketed() -> None:
    kwargs = _connect_kwargs("::1", 9001)
    assert kwargs["address"] == "[::1]:9001", (
        f"IPv6 host must be bracketed before passing to dbapi; got {kwargs['address']!r}"
    )
    # And the dbapi parser accepts the result without raising.
    assert _parse_address(str(kwargs["address"])) == ("::1", 9001)


def test_ipv6_full_address_is_bracketed() -> None:
    kwargs = _connect_kwargs("2001:db8::1", 9001)
    assert kwargs["address"] == "[2001:db8::1]:9001"
    assert _parse_address(str(kwargs["address"])) == ("2001:db8::1", 9001)


def test_ipv6_global_unicast_is_bracketed() -> None:
    kwargs = _connect_kwargs("2001:db8:85a3::8a2e:370:7334", 9001)
    assert kwargs["address"] == "[2001:db8:85a3::8a2e:370:7334]:9001"


def test_ipv4_address_unchanged() -> None:
    kwargs = _connect_kwargs("127.0.0.1", 9001)
    assert kwargs["address"] == "127.0.0.1:9001"


def test_dns_hostname_unchanged() -> None:
    kwargs = _connect_kwargs("node1.example.com", 9001)
    assert kwargs["address"] == "node1.example.com:9001"


def test_default_localhost_unchanged() -> None:
    # No ``host=`` passed; SA defaults pass through; dialect uses
    # ``localhost``.
    url = URL.create("dqlite", database="test")
    _, kwargs = DqliteDialect().create_connect_args(url)
    assert kwargs["address"] == "localhost:9001"


@pytest.mark.parametrize(
    "ipv6_host",
    [
        "::1",
        "::",
        "2001:db8::1",
        "fe80::1",
        "2001:db8:85a3::8a2e:370:7334",
    ],
)
def test_ipv6_addresses_round_trip_through_dbapi_parser(ipv6_host: str) -> None:
    """Every well-formed IPv6 host produced by the dialect must be
    accepted by ``_parse_address`` and round-trip back to the original
    host."""
    kwargs = _connect_kwargs(ipv6_host, 9001)
    parsed_host, parsed_port = _parse_address(str(kwargs["address"]))
    assert parsed_host == ipv6_host
    assert parsed_port == 9001
