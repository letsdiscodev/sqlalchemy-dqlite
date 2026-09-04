# Transactions

SQLAlchemy owns `BEGIN` / `COMMIT` / `ROLLBACK` for any block opened via
`engine.begin()`, `connection.begin()`, or `session.begin()`. **Do not issue
a raw `BEGIN` yourself** — this is the standard SQLAlchemy model, and it is
the same on every backend (pysqlite, PostgreSQL, MySQL).

```python
from sqlalchemy import create_engine, text

engine = create_engine("dqlite://localhost:9001/mydb")

# OK — SQLAlchemy emits BEGIN / COMMIT for you:
with engine.begin() as conn:
    conn.execute(text("INSERT INTO t VALUES (1)"))

# WRONG — a manual BEGIN inside an SA-managed transaction errors with
#   "cannot start a transaction within a transaction":
with engine.begin() as conn:
    conn.execute(text("BEGIN"))                    # error
    conn.execute(text("INSERT INTO t VALUES (1)"))
```

The same applies to `engine.connect()`: SQLAlchemy auto-begins on the first
execute, so a user-issued `text("BEGIN")` collides the same way.

## Session modes

The dialect emits a bare `BEGIN`; the dbapi qualifies it according to the
connection's session mode (`BEGIN IMMEDIATE` by default, so a read-then-write
block cannot lose its snapshot to a concurrent writer). The engine-wide default
comes from the `session_mode` connect argument (`?session_mode=` in the URL or
`connect_args`); a per-connection override is the `dqlite_session_mode`
execution option:

```python
with engine.connect().execution_options(dqlite_session_mode="read_only") as conn:
    conn.execute(text("SELECT ..."))          # writes raise OperationalError here
```

Accepted values are `immediate`, `deferred`, `exclusive`, and `read_only`.
The option is transactional: it cannot be changed inside an open transaction,
and the pool restores the connect-time default on checkin.

## No `AUTOCOMMIT`

`isolation_level="AUTOCOMMIT"` is rejected. Every dqlite statement goes
through Raft consensus and there is no per-statement autocommit mode — use
`engine.begin()` (or `connection.begin()`) for writes. Isolation is always
SERIALIZABLE.

See SQLAlchemy's
[transaction docs](https://docs.sqlalchemy.org/en/20/core/connections.html#using-transactions)
for the full model.

## Savepoints

SQLAlchemy's `Session.begin_nested()` / `connection.begin_nested()` use
generated savepoint names (e.g. `sa_savepoint_1`), which the dialect tracks
correctly — no action needed.

Only **raw** SQL with an unusual savepoint name (quoted, backticked,
bracketed, unicode, or leading-digit, e.g. `text('SAVEPOINT "weird name"')`)
falls outside the tracker. When that happens the connection is conservatively
flagged as carrying an untracked savepoint, and SQLAlchemy issues a safety
`ROLLBACK` on the next pool checkin — one extra round-trip per checkout for
the rest of that connection's life in the pool. Stick to bare-ASCII savepoint
names in raw SQL to avoid it.
