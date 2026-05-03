# Development Guide

## Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager
- Docker (for integration tests)

## Setup

Clone [dqlite-wire](https://github.com/letsdiscodev/python-dqlite-wire), [dqlite-client](https://github.com/letsdiscodev/python-dqlite-client), and [dqlite-dbapi](https://github.com/letsdiscodev/python-dqlite-dbapi) alongside this checkout; `[tool.uv.sources]` in `pyproject.toml` points the sibling packages at their `../python-dqlite-*` directories, so `uv sync` picks up in-tree changes automatically.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync --extra dev
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

The default pytest run **skips** `tests/compliance/` (the SQLAlchemy
dialect compliance suite). See "SQLAlchemy compliance suite" below
for the why and how to run it.

## SQLAlchemy compliance suite

The SQLAlchemy project ships an in-tree, dialect-agnostic test suite
at `sqlalchemy.testing.suite`. Third-party dialects engage it by
providing a `Requirements` class (declares which features are
supported), a `provision.py` module (handles per-test database
lifecycle), and a `tests/test_suite.py` that imports the suite. SA's
pytest plugin then runs every suite test against the dialect, gated
by `Requirements.<feature>.enabled`.

This package wires the suite under `tests/compliance/`. Running it:

```bash
# Start the cluster (any reachable dqlite cluster works; the suite
# defaults to localhost:9001 via setup.cfg's [db] section).
.venv/bin/pytest tests/compliance/
```

### Why the default pytest run skips it

`tests/compliance/conftest.py` loads `sqlalchemy.testing.plugin.pytestplugin`,
which **replaces pytest's default test-collection model** — it filters
discovery to `sqlalchemy.testing.fixtures.TestBase` subclasses only.
If we let it load globally for `pytest tests/`, the project's existing
unit and integration tests under `tests/test_*.py` would silently
disappear from the run.

The plugin also requires CLI options (`--dburi`, etc.) the project's
runners do not pass; without them it errors at session start.

To keep the two test corpora cleanly separated, `pyproject.toml`'s
pytest config has `addopts=["--ignore=tests/compliance"]`. Run the
suite explicitly with `pytest tests/compliance/`.

### Why ~700 suite tests are skipped (and that's correct)

Each compliance run reports a large `skipped` count (currently
~689). Every skip is gated by a `Requirements.<feature>` declaration
in `src/sqlalchemydqlite/requirements.py` that says "dqlite doesn't
support this." The skipped tests are testing capabilities the
underlying database genuinely lacks — running them would fail with
`syntax error` / `unknown database` / `no such function`, not with
real bugs.

Examples of features dqlite doesn't have (each declared
`exclusions.closed()`):

- ATTACH-DATABASE schemas (`schemas`)
- Two-phase commit (`two_phase_transactions`)
- `CREATE SEQUENCE` (`sequences` — SQLite has no native sequences)
- Datetime literals (`datetime_literals`)
- Parenthesised SELECTs in UNION (`parens_in_union_*`)
- Cross-schema FK reflection (`cross_schema_fk_reflection`)

A future dqlite version that adds the feature flips the requirement
to `exclusions.open()`, and the suite tests for that feature start
running.

### Adjusting Requirements

When the suite surfaces a real bug, fix the dialect. When the suite
runs a test for a capability dqlite genuinely doesn't have, add or
adjust a `Requirements.<name>` property in `requirements.py` with a
docstring explaining why. The matching property in SA's
`SuiteRequirements` has a docstring describing what the feature is
— mirror that in the override.

### Per-session unique database names

dqlite has no `DROP DATABASE` primitive. The cluster persists every
database name ever opened, across pytest sessions and across the
project's other integration tests. To prevent cross-run schema
bleed in reflection tests that enumerate "all tables in the schema",
`src/sqlalchemydqlite/provision.py` generates a per-session token
(`sa_<pid>_<monotonic-ns>`) and suffixes every test database name
with it. Each pytest run gets a fresh, empty database namespace on
the cluster.

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
