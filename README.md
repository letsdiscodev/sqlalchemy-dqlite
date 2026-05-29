# sqlalchemy-dqlite

A [SQLAlchemy 2.0](https://www.sqlalchemy.org/) dialect for
[dqlite](https://dqlite.io/), Canonical's distributed SQLite. Point
SQLAlchemy (and Alembic, and most ORMs built on it) at a dqlite cluster
with a `dqlite://` URL and use it like any other backend.

It supports both the synchronous and the async SQLAlchemy engines, and
builds on dqlite's SQLite roots, so SQLAlchemy treats it as a SQLite-family
dialect.

## Is this the package you want?

Yes, if you use SQLAlchemy or an ORM/migration tool on top of it (Alembic,
Flask-SQLAlchemy, etc.) and want it backed by dqlite. If you want a plain
database driver without SQLAlchemy, use
[dqlite-dbapi](https://github.com/letsdiscodev/python-dqlite-dbapi).

## Installation

```bash
pip install sqlalchemy-dqlite
```

Requires Python 3.13+ and SQLAlchemy 2.0+.

## Usage

```python
from sqlalchemy import create_engine, text

engine = create_engine("dqlite://localhost:9001/mydb")
with engine.connect() as conn:
    print(conn.execute(text("SELECT 1")).fetchone())
```

Async:

```python
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("dqlite+aio://localhost:9001/mydb")
async with engine.connect() as conn:
    result = await conn.execute(text("SELECT 1"))
    print(result.fetchone())
```

URL format: `dqlite://host:port/database` (or `dqlite+aio://…` for async).
The host:port is the bootstrap node — the cluster is discovered from there.
See [Connection URL](docs/connection-url.md) for details.

## The dqlite Python stack

This is the top of four layered packages. Each builds on the one below:

| Package | Role |
| --- | --- |
| **sqlalchemy-dqlite** — this package | SQLAlchemy 2.0 dialect |
| [dqlite-dbapi](https://github.com/letsdiscodev/python-dqlite-dbapi) | PEP 249 (DB-API 2.0) driver — sync & async |
| [dqlite-client](https://github.com/letsdiscodev/python-dqlite-client) | Async wire client — pooling, leader discovery |
| [dqlite-wire](https://github.com/letsdiscodev/python-dqlite-wire) | Wire-protocol codec |

## Documentation

- [Transactions](docs/transactions.md) — SQLAlchemy owns `BEGIN`/`COMMIT`; no `AUTOCOMMIT`.
- [Connection URL](docs/connection-url.md) — URL format, the bootstrap address, and query parameters.
- [Limitations & notes](docs/limitations.md) — STRICT tables, savepoint
  naming, and a server-version NULL gotcha.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup, contribution guidelines, and
how to run the SQLAlchemy compliance test suite.

## License

MIT
