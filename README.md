# sqlalchemy-dqlite

SQLAlchemy 2.0 dialect for [dqlite](https://dqlite.io/).

## Installation

```bash
pip install sqlalchemy-dqlite
```

## Usage

```python
from sqlalchemy import create_engine, text

# Sync
engine = create_engine("dqlite://localhost:9001/mydb")
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.fetchone())

# Async
from sqlalchemy.ext.asyncio import create_async_engine

async_engine = create_async_engine("dqlite+aio://localhost:9001/mydb")
async with async_engine.connect() as conn:
    result = await conn.execute(text("SELECT 1"))
    print(result.fetchone())
```

## Transactions

SQLAlchemy owns the `BEGIN`/`COMMIT`/`ROLLBACK` for any block opened via
`engine.begin()`, `connection.begin()`, or `session.begin()`. Do not
issue raw `BEGIN` yourself.

```python
from sqlalchemy import create_engine, text

engine = create_engine("dqlite://localhost:9001/mydb")

# OK — SA emits BEGIN / COMMIT for you
with engine.begin() as conn:
    conn.execute(text("INSERT INTO t VALUES (1)"))

# WRONG — second BEGIN inside an SA-managed transaction errors with
#   OperationalError: cannot start a transaction within a transaction
with engine.begin() as conn:
    conn.execute(text("BEGIN"))                  # error
    conn.execute(text("INSERT INTO t VALUES (1)"))
```

The same rule applies to `engine.connect()`: SA auto-begins a
transaction on the first execute, so a user-issued `text("BEGIN")`
collides the same way. This matches every other SA backend (pysqlite,
postgres, mysql); SA's transaction model is universal.

`isolation_level="AUTOCOMMIT"` is rejected — every dqlite statement
goes through Raft consensus and there is no per-statement autocommit
mode. Use `engine.begin()` (or `connection.begin()`) for writes.

See SQLAlchemy's [transaction
docs](https://docs.sqlalchemy.org/en/20/core/connections.html#using-transactions)
for the full model.

## URL Format

```
dqlite://host:port/database
dqlite+aio://host:port/database
```

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup and contribution guidelines.

## License

MIT
