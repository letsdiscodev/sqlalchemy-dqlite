# Development Guide

## Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager
- Docker (for integration tests)

## Setup

Start by also cloning [dqlite-wire](https://github.com/letsdiscodev/python-dqlite-wire),
[dqlite-client](https://github.com/letsdiscodev/python-dqlite-client)
and [dqlite-dbapi](https://github.com/letsdiscodev/python-dqlite-dbapi).

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv --python 3.13
uv pip install -e "../python-dqlite-wire" -e "../python-dqlite-client" -e "../python-dqlite-dbapi" -e ".[dev]"
```

## Development Tools

| Tool | Purpose | Command |
|------|---------|---------|
| **pytest** | Testing framework | `pytest` |
| **ruff** | Linter (replaces flake8, isort, etc.) | `ruff check` |
| **ruff format** | Code formatter (replaces black) | `ruff format` |
| **mypy** | Static type checker | `mypy src` |

## Running Tests

```bash
# Run unit tests only
.venv/bin/pytest tests/ --ignore=tests/integration

# Run all tests (requires Docker cluster)
cd ../dqlite-test-cluster && docker compose up -d
.venv/bin/pytest tests/
```

## Linting & Formatting

```bash
# Lint
.venv/bin/ruff check src tests

# Auto-fix lint issues
.venv/bin/ruff check --fix src tests

# Format
.venv/bin/ruff format src tests
```

## Type Checking

```bash
.venv/bin/mypy src
```

## Pre-commit Workflow

```bash
.venv/bin/ruff format src tests
.venv/bin/ruff check --fix src tests
.venv/bin/mypy src
.venv/bin/pytest tests/ --ignore=tests/integration
```

## SQLAlchemy URL Format

```
# Sync
dqlite://host:port/database

# Async
dqlite+aio://host:port/database
```

## Dialect Registration

The dialects are registered via entry points in `pyproject.toml`:

```toml
[project.entry-points."sqlalchemy.dialects"]
dqlite = "sqlalchemydqlite:DqliteDialect"
"dqlite.aio" = "sqlalchemydqlite.aio:DqliteDialect_aio"
```
