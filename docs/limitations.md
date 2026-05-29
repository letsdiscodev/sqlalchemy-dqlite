# Limitations & notes

## STRICT tables are not emitted by the compiler

SQLAlchemy's SQLite dialect gates `CREATE TABLE ... STRICT` compilation on
`server_version_info >= (3, 37)`. This dialect pins
`sqlite_version_info = (3, 35, 0)` as a documented floor so it doesn't reject
connections to older-server clusters. As a result, SQLAlchemy's compiler will
**not** emit STRICT DDL through this dialect, even when your cluster ships
SQLite 3.37+.

If your cluster supports STRICT tables and you want them, emit the DDL as raw
SQL (`engine.execute(text("CREATE TABLE ... STRICT"))`) rather than through
SQLAlchemy's `Table` / `Column` model.

## NULL in Boolean/DateTime columns depends on the server version

A 2026 dqlite server change (upstream commit `f30fc99`) changed how a NULL
cell is encoded in columns declared `BOOLEAN`, `DATE`, `DATETIME`, or
`TIMESTAMP`:

- **Before**: a NULL came back as `False` (Boolean) or `""` (datetime
  types) — indistinguishable from a real value.
- **After**: a NULL comes back as `None`.

For ORM models this means a `Boolean()` / `DateTime()` column that used to
return `False` / `""` will start returning `None` after a server upgrade, and
`nullable=False` constraints may start tripping on rows that previously
decoded to non-NULL. There is no handshake distinguishing the two server
versions — check your dqlite cluster version before relying on the post-change
semantics.
