# mypy: ignore-errors
"""SQLAlchemy testing provision hooks for the dqlite dialect.

The compliance suite under ``sqlalchemy.testing.suite`` discovers this
module via ``DefaultDialect.load_provisioning`` and dispatches the
hooks below by backend name (``"dqlite"``).

Lifecycle model
===============

Unlike a full RDBMS, dqlite has no ``CREATE DATABASE`` / ``DROP
DATABASE`` primitives. Each "database" is a name-keyed string that the
client sends in ``OpenRequest``; the cluster materialises the database
on first open and persists it for the cluster's lifetime. Two
consequences shape this provision module:

1. ``create_db`` is a no-op — opening a fresh URL with a unique
   ``database`` name auto-creates it.
2. ``drop_db`` cannot remove the database from the server. Best we can
   do is drop every user-visible schema object so a subsequent run
   against the same cluster (sharing the same database name) starts
   from a clean schema. The compliance suite uses follower idents to
   suffix database names, so cross-run collision is bounded; this
   helper is a safety net.

Cluster vs filesystem
=====================

SA's pysqlite provision uses filesystem deletion to drop databases.
dqlite has no on-disk file the test harness owns — the cluster is in
the test fixture's docker-compose. So:

- ``create_db``: no-op.
- ``drop_db`` / ``run_reap_dbs``: drop user tables from the test
  schema so the database is logically empty; rely on the cluster
  fixture to be fresh for full state-reset.
- ``temp_table_keyword_args``: ``TEMPORARY`` (SQLite ``TEMP TABLE``
  semantics carry through dqlite's wire).
- ``follower_url_from_main`` / ``generate_driver_url``: rewrite the
  URL with a follower-suffixed database name and a normalised
  driver string.
"""

import logging
import os
import time
from typing import Any, Final

# SA's compliance suite discovers the ``_dqlite_*`` functions below
# via ``@register.init`` hook decorators, not via ``__all__``. Nothing
# here is intended for direct user import — declare ``__all__`` empty
# so ``from sqlalchemydqlite.provision import *`` correctly leaks
# nothing, and the workspace's submodule-``__all__`` discipline
# applies uniformly.
__all__: list[str] = []

from sqlalchemy.engine import url as sa_url
from sqlalchemy.testing.provision import (
    create_db,
    drop_all_schema_objects_post_tables,
    drop_db,
    follower_url_from_main,
    generate_driver_url,
    run_reap_dbs,
    stop_test_class_outside_fixtures,
    temp_table_keyword_args,
    upsert,
)

from dqlitewire import sanitize_for_log as _sanitize_for_log

# Per-session unique database-name suffix. dqlite has no
# ``DROP DATABASE``: reusing the cluster's ``default`` database across
# pytest sessions would leak prior-session schema (every table the
# suite or our other integration tests created). Append a session-
# unique token at URL-rewrite time so each pytest run gets a fresh,
# empty database namespace on the cluster.
#
# Token shape: ``sa_<pid>_<monotonic-ns>`` — bounded length, safe
# across the dbapi/URL/wire layers (no path separators, no ``@``, no
# whitespace), and sufficiently unique for parallel pytest runs that
# share a cluster fixture. Module-level so the value is stable across
# every URL rewrite within a single pytest session.
_SESSION_TOKEN: Final[str] = f"sa_{os.getpid()}_{time.monotonic_ns()}"

# The SA testing API documents ``log`` / ``logger`` as the canonical
# place provision helpers chatter to. Use ``__name__`` (matching the
# rest of the package) so vendored / namespace-relocated installs see
# their actual module path in log records, not a hardcoded string.
logger = logging.getLogger(__name__)


# Drivers we register under the ``dqlite`` backend.
#
# SA's ``generate_driver_url`` invokes this hook with the value
# returned by ``URL.get_driver_name()``. That method returns the
# dialect's ``driver`` class attribute — ``"dqlitedbapi"`` for the
# sync dialect (see ``base.py``) and ``"aio"`` for the async dialect
# (see ``aio.py``). A URL of the form ``dqlite://...`` (no
# ``+driver``) reports ``"dqlitedbapi"`` because SA defaults to the
# dialect's ``driver`` when no explicit driver is given.
#
# Accept all three forms so ``--dburi`` / ``--dbs`` work with any of
# the canonical shapes:
#
#   dqlite://...               -> driver name "dqlitedbapi"
#   dqlite+dqlitedbapi://...   -> driver name "dqlitedbapi"
#   dqlite+aio://...           -> driver name "aio"
#
# The "dqlite" entry is a tolerated alias used internally when we
# rewrite URLs from the bare-dialect form back into themselves; SA
# never invokes the hook with that value at the call site, but
# keeping it here documents the canonical sync drivername the
# rewrite produces.
_DRIVERNAMES: Final[frozenset[str]] = frozenset({"dqlite", "dqlitedbapi", "aio"})


def _format_url(url: sa_url.URL, driver: str | None, ident: str | None) -> sa_url.URL:
    """Rewrite ``url`` for a specific test driver and follower ident.

    - ``driver`` selects the driver part of ``dialectname+drivername``.
      ``None`` keeps the URL's existing driver.
    - ``ident`` is appended to the database name so concurrent test
      followers (pytest-xdist workers, parallel suites) get distinct
      database namespaces on the same cluster. ``None`` leaves the
      database name unchanged.
    """
    if driver is None:
        # Pull from the URL's drivername; SA stores it as
        # ``dqlite`` or ``dqlite+aio``.
        existing = url.drivername.split("+", 1)
        driver = existing[1] if len(existing) == 2 else "dqlite"

    # Canonical dialect+driver form for the rewritten URL.
    # ``dqlite`` and ``dqlitedbapi`` both map back to the bare sync
    # form (the dialect's class-attr ``driver = "dqlitedbapi"`` is
    # implied when no ``+driver`` is in the URL string); ``aio``
    # produces the explicit ``dqlite+aio://...`` form.
    new_drivername = "dqlite" if driver in ("dqlite", "dqlitedbapi") else f"dqlite+{driver}"

    database = url.database or "default"
    # Suffix with a session-unique token so each pytest run uses a
    # fresh database name on the cluster. dqlite has no ``DROP
    # DATABASE`` primitive — without per-session uniqueness, cross-run
    # state (tables from prior runs of this suite or of the project's
    # other integration tests) bleeds into reflection tests that
    # enumerate "all tables in the schema".
    #
    # SA's bootstrap calls ``_format_url`` twice along the same chain
    # (``generate_driver_url`` first with ``ident=None``, then
    # ``follower_url_from_main`` with the worker ident); without the
    # already-suffixed guard below, the session token gets appended
    # twice, producing ``db_sa_<pid>_<ts>_sa_<pid>_<ts>_gw0``. Detect
    # the prior suffix and only append the missing pieces.
    if _SESSION_TOKEN in database:
        # Prior pass already attached the session token. Append the
        # follower ident only.
        if ident:
            ident_clean = ident.replace("/", "_").replace("@", "_")
            ident_suffix = f"_{ident_clean}"
            if not database.endswith(ident_suffix):
                database = f"{database}{ident_suffix}"
    else:
        suffix = _SESSION_TOKEN
        if ident:
            # Avoid ``@`` / path-separators / control chars so the
            # resulting name parses cleanly across the dbapi/URL/wire
            # layers.
            ident_clean = ident.replace("/", "_").replace("@", "_")
            suffix = f"{suffix}_{ident_clean}"
        database = f"{database}_{suffix}"

    return url.set(drivername=new_drivername, database=database)


@generate_driver_url.for_db("dqlite")
def _dqlite_generate_driver_url(
    url: sa_url.URL, driver: str, query_str: str | None
) -> sa_url.URL | None:
    """Return ``url`` rewritten for the requested driver, or ``None``
    if the driver isn't recognised (SA's dispatch then skips it).

    Mirrors the pysqlite hook: a single base URL fans out to one
    URL per registered driver. Currently ``dqlite`` (sync) and
    ``aio`` (async) are the two we register.
    """
    if driver not in _DRIVERNAMES:
        return None
    rewritten = _format_url(url, driver, None)
    # Surface unrecognised dialect drivers explicitly. SA's helper
    # otherwise returns the URL even when ``import_dbapi`` would later
    # raise ``NoSuchModuleError`` at engine construction.
    try:
        rewritten.get_dialect()
    except Exception:
        return None
    return rewritten


@follower_url_from_main.for_db("dqlite")
def _dqlite_follower_url_from_main(url: sa_url.URL, ident: str) -> sa_url.URL:
    """Rewrite the main URL for a parallel-test follower.

    ``ident`` is e.g. ``test_a1b2c3d4`` — pytest-xdist worker id,
    or a session-unique token for a non-xdist suite. Each follower
    gets its own database name so the suite can run multiple workers
    against the same cluster without state collisions.
    """
    return _format_url(url, None, ident)


@create_db.for_db("dqlite")
def _dqlite_create_db(cfg: Any, eng: Any, ident: str) -> None:
    """No-op: dqlite materialises a database on first ``Open`` request.

    The compliance suite invokes this before opening the engine; for
    dqlite, simply opening with the rewritten URL (post
    ``follower_url_from_main``) auto-creates the database on the
    cluster. Nothing to provision up front.
    """
    logger.info("dqlite create_db: no-op for ident=%r", ident)


def _drop_user_tables(eng: Any) -> None:
    """Best-effort drop of every user-visible table in the dialect's
    default schema.

    dqlite has no ``DROP DATABASE``; this is the closest equivalent.
    Used by ``drop_db`` and ``run_reap_dbs``. Errors are swallowed
    (logged at debug) because individual drops can fail under
    cross-test schema drift; the next run's ``CREATE TABLE IF NOT
    EXISTS`` semantics absorb residual state. The cluster fixture is
    expected to be the authoritative reset boundary.
    """
    try:
        with eng.connect() as conn:
            tables = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            for (name,) in tables:
                # SQLite escapes ``"`` inside a delimited identifier as
                # ``""``. A peer / test creating a table named
                # ``foo"bar`` would otherwise render as
                # ``DROP TABLE IF EXISTS "foo"bar"`` — a syntax error
                # that the per-drop swallow below would mask, silently
                # half-completing the docstring's "drop every user-
                # visible table" contract.
                quoted = '"' + name.replace('"', '""') + '"'
                try:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {quoted}")
                except Exception as e:
                    # CWE-117: both the bubbled-up exception text and
                    # the table ``name`` from ``sqlite_master`` can
                    # carry peer-supplied LF/CR. Route both through
                    # ``sanitize_for_log`` so journald/syslog cannot
                    # interpret forged LF as a record boundary. The
                    # sibling ``_dqlite_run_reap_dbs`` arm applies the
                    # same discipline.
                    logger.debug(
                        "drop_user_tables: %s on DROP TABLE %s",
                        _sanitize_for_log(str(e)),
                        _sanitize_for_log(str(name)),
                    )
            conn.commit()
    except Exception as e:
        logger.debug(
            "drop_user_tables: %s during connect/exec",
            _sanitize_for_log(str(e)),
        )


@drop_db.for_db("dqlite")
def _dqlite_drop_db(cfg: Any, eng: Any, ident: str) -> None:
    """Drop user tables from the per-follower database.

    dqlite cannot drop the database itself; the database name persists
    on the cluster until the cluster is recycled. Drop all user tables
    so the database is logically empty — the next test run that picks
    the same name sees a clean schema.
    """
    logger.info("dqlite drop_db: dropping user tables for ident=%r", ident)
    _drop_user_tables(eng)


@drop_all_schema_objects_post_tables.for_db("dqlite")
def _dqlite_drop_all_schema_objects_post_tables(cfg: Any, eng: Any) -> None:
    """SA hook called by the suite after dropping suite-managed tables.

    For dqlite this is symmetric with ``drop_db``: drop any user-
    visible table the suite created and didn't track. Idempotent.
    """
    _drop_user_tables(eng)


@run_reap_dbs.for_db("dqlite")
def _dqlite_run_reap_dbs(url: str | sa_url.URL, idents: list[str]) -> None:
    """Reap follower databases at suite end.

    Without a real ``DROP DATABASE``, the best we can do is drop user
    tables in each follower's database so a subsequent suite run sees
    a logically clean state. Operators that want full state reset
    should restart the cluster between suite runs.
    """
    from sqlalchemy import create_engine

    parsed = sa_url.make_url(url) if isinstance(url, str) else url
    logger.info("dqlite reap_dbs: %d follower(s) at %s", len(idents), parsed)
    for ident in idents:
        # Force the sync drivername regardless of the input URL's
        # ``+driver`` suffix. ``_drop_user_tables`` uses sync
        # ``eng.connect()`` — if the input was ``dqlite+aio://``,
        # the unforced ``_format_url(parsed, None, ident)`` would
        # carry the ``+aio`` suffix and ``create_engine`` would
        # route to the async dialect that requires
        # ``create_async_engine``. The sync rewrite via the
        # ``"dqlitedbapi"`` driver token resolves to the bare
        # ``"dqlite"`` drivername (``_format_url`` line 128 maps
        # both ``"dqlite"`` and ``"dqlitedbapi"`` to the bare form).
        follower_url = _format_url(parsed, "dqlitedbapi", ident)
        try:
            eng = create_engine(follower_url)
            try:
                _drop_user_tables(eng)
            finally:
                eng.dispose()
        except Exception as e:
            # CWE-117: peer-supplied text can reach `str(e)` (e.g. an
            # `OperationalError` wrapping a server-emitted message that
            # the wire layer keeps LF-preserved). Route both interpolations
            # through `sanitize_for_log` so journald/syslog cannot
            # interpret forged LF as a record boundary. Every other log
            # site in this package applies the same discipline.
            logger.debug(
                "reap_dbs ident=%s: %s",
                _sanitize_for_log(str(ident)),
                _sanitize_for_log(str(e)),
            )


@stop_test_class_outside_fixtures.for_db("dqlite")
def _dqlite_stop_test_class_outside_fixtures(config: Any, db: Any, cls: Any) -> None:
    """Dispose the engine after each test class.

    Mirrors the pysqlite hook. dqlite connections are TCP sockets to
    the cluster; disposing the engine releases them promptly between
    test classes rather than waiting for GC.
    """
    db.dispose()


@temp_table_keyword_args.for_db("dqlite")
def _dqlite_temp_table_keyword_args(cfg: Any, eng: Any) -> dict[str, Any]:
    """Use SQLite ``TEMPORARY`` table prefix.

    dqlite's wire passes through to SQLite; ``TEMP TABLE`` semantics
    apply per-connection. The compliance suite's temp-table tests
    use this hook to choose the correct DDL prefix.
    """
    return {"prefixes": ["TEMPORARY"]}


@upsert.for_db("dqlite")
def _dqlite_upsert(
    cfg: Any,
    table: Any,
    returning: Any,
    *,
    set_lambda: Any = None,
    sort_by_parameter_order: bool = False,
    index_elements: Any = None,
) -> Any:
    """Build a dialect-specific upsert statement for SA's testing harness.

    dqlite IS SQLite at the SQL surface — INSERT … ON CONFLICT … DO
    UPDATE / DO NOTHING, plus RETURNING, are inherited from the
    SQLiteCompiler. Reuse pysqlite's ``insert`` constructor verbatim
    rather than wrapping a backend-specific shim.

    The body mirrors ``sqlalchemy/dialects/sqlite/provision.py:_upsert``
    so behaviour stays in lockstep if SA refactors the hook contract.
    Registration pairs with the public re-export of ``Insert`` / ``insert``
    from ``sqlalchemydqlite.__init__``.
    """
    from sqlalchemy.dialects.sqlite import insert

    stmt = insert(table)
    if set_lambda is not None:
        stmt = stmt.on_conflict_do_update(set_=set_lambda(stmt.excluded))
    else:
        stmt = stmt.on_conflict_do_nothing()
    stmt = stmt.returning(*returning, sort_by_parameter_order=sort_by_parameter_order)
    return stmt
