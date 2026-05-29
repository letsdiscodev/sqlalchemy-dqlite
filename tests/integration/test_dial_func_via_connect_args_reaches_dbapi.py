"""Pin: ``dial_func`` via SA ``connect_args=`` reaches the dbapi and is invoked.

A custom dialer is the way to inject TLS / unix-socket / test transport; a
regression dropping it from the allowlist or failing to forward it lands silently.
"""

from __future__ import annotations

import asyncio

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from dqliteclient._dial import open_connection_with_keepalive


def test_sync_dial_func_via_connect_args_routed_to_dbapi(engine_url: str) -> None:
    invocations: list[str] = []

    async def recording_dialer(address: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        invocations.append(address)
        host, port_str = address.rsplit(":", 1)
        return await open_connection_with_keepalive(host, int(port_str))

    engine = create_engine(
        engine_url,
        connect_args={"dial_func": recording_dialer},
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        assert invocations, (
            "custom dial_func passed via connect_args did NOT reach "
            "the dbapi layer; SA dialect's create_connect_args must "
            "forward the kwarg verbatim"
        )
    finally:
        engine.dispose()


def test_async_dial_func_via_connect_args_routed_to_dbapi(async_engine_url: str) -> None:
    """Async dialect mirror."""
    invocations: list[str] = []

    async def recording_dialer(address: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        invocations.append(address)
        host, port_str = address.rsplit(":", 1)
        return await open_connection_with_keepalive(host, int(port_str))

    async def _run() -> None:
        engine = create_async_engine(
            async_engine_url,
            connect_args={"dial_func": recording_dialer},
        )
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        finally:
            await engine.dispose()

    asyncio.run(_run())
    assert invocations, (
        "custom dial_func passed via connect_args did NOT reach the "
        "dbapi layer on the async dialect"
    )
