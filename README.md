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

## Savepoint naming

The dqlite client tracks active SAVEPOINTs to keep the SQLAlchemy
pool's ROLLBACK-on-checkin path correct. The tracker only handles
bare-ASCII SQLite identifiers (e.g. `sa_savepoint_1`, `my_sp`) —
SQLAlchemy's generated savepoint names always match this shape, so
`engine.begin()` / `Session.begin_nested()` / `connection.begin_nested()`
are unaffected.

If user-issued raw SQL uses quoted, backticked, square-bracketed,
unicode, or leading-digit savepoint names (e.g.
`text('SAVEPOINT "weird name"')`), the client conservatively flags
the connection as carrying an untracked savepoint. On the next pool
checkin SQLAlchemy issues a safety `ROLLBACK`, paying one extra
round-trip per checkout for the remainder of that connection's
lifetime in the pool. Stick to bare-ASCII SAVEPOINT names in raw
text SQL to avoid the overhead, or accept the per-checkout cost.

## URL Format

```
dqlite://host:port/database
dqlite+aio://host:port/database
```

When a query parameter is repeated
(`?max_total_rows=100&max_total_rows=200`), the **last** occurrence
wins. This matches `urllib.parse.parse_qsl` ordering. Templated
connection URLs that layer values from multiple sources should be
aware that duplicated keys silently override earlier values rather
than raising.

The URL host:port pair is the bootstrap address — the dqlite client
discovers the rest of the cluster from that one node's leader-info
response. If the URL host is unreachable, leader-discovery cannot
start; operators that want bootstrap-from-many-addresses should put
a load balancer or DNS round-robin in front of the cluster, or rotate
the URL host across deployments. Multi-address bootstrap is not
exposed at the dialect URL surface.

## Cross-version semantic shift: NULL in BOOLEAN/DATETIME columns

Upstream dqlite commit `f30fc99` (`query: preserve SQLITE_NULL type
for NULL values`, 2026-01-25) changed the wire encoding of NULL cells
in columns declared `BOOLEAN`, `DATE`, `DATETIME`, or `TIMESTAMP`.
Before that commit, a NULL in a `BOOLEAN` column was emitted as
`BOOLEAN(0)` (decodes to `False`) and a NULL in a `DATETIME` column
was emitted as `ISO8601("")` — indistinguishable on the wire from
real `FALSE` / empty-string values. After the commit, NULL is emitted
with the SQLite NULL type and decodes to `None`.

ORM models with `Boolean()` and `DateTime()` columns will start
returning `None` for previously-`False` / `""` values after a server
upgrade. SQLAlchemy `nullable=False` constraints will start tripping
on rows that previously decoded to non-NULL. There is no driver-level
handshake distinguishing the two server versions — check your dqlite
cluster version before relying on the post-fix semantics.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup, contribution guidelines,
and the SQLAlchemy compliance test-suite documentation (how to run it
locally, why it's not part of the default `pytest` invocation, and
why ~700 of its tests are correctly skipped).

## License

MIT
