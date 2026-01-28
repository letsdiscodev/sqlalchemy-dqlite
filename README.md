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

## URL Format

```
dqlite://host:port/database
dqlite+aio://host:port/database
```

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup and contribution guidelines.

## License

MIT
