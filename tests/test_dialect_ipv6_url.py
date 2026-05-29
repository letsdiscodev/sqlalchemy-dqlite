"""Pin: the dialect re-brackets IPv6 hosts in the ``address`` kwarg. SA's URL parser strips
the brackets, but the client's ``_parse_address`` needs them to disambiguate host from port."""

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
    url = URL.create("dqlite", database="test")
    _, kwargs = DqliteDialect().create_connect_args(url)
    assert kwargs["address"] == "localhost:9001"


@pytest.mark.parametrize(
    "ipv6_host",
    [
        "::1",
        # ``::`` (unspecified) omitted: ``_parse_address`` rejects it (TCP can't target it).
        "2001:db8::1",
        "fe80::1",
        "2001:db8:85a3::8a2e:370:7334",
    ],
)
def test_ipv6_addresses_round_trip_through_dbapi_parser(ipv6_host: str) -> None:
    kwargs = _connect_kwargs(ipv6_host, 9001)
    parsed_host, parsed_port = _parse_address(str(kwargs["address"]))
    assert parsed_host == ipv6_host
    assert parsed_port == 9001
