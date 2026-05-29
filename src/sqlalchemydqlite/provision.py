# mypy: ignore-errors
"""SQLAlchemy testing provision hooks for the dqlite dialect.

dqlite has no CREATE/DROP DATABASE: a database is materialised on first
open and persists for the cluster's lifetime. So ``create_db`` is a
no-op and ``drop_db``/``run_reap_dbs`` only drop user schema objects to
leave the per-database state logically empty; full state reset relies
on a fresh cluster fixture.
"""

import contextlib
import logging
import os
import re
import time
from typing import Any, Final

# Hooks are discovered via decorators, not export; nothing is for user import.
__all__: list[str] = []

from sqlalchemy.engine import url as sa_url
from sqlalchemy.testing.provision import (
    create_db,
    drop_all_schema_objects_post_tables,
    drop_db,
    follower_url_from_main,
    generate_driver_url,
    post_configure_engine,
    run_reap_dbs,
    stop_test_class_outside_fixtures,
    temp_table_keyword_args,
    upsert,
)

from dqlitewire import sanitize_for_log as _sanitize_for_log

# Cap so a server-controlled value cannot allocate an unbounded log record.
_LOG_TRUNCATE_MAX_CHARS: Final[int] = 200


def _safe_for_log(value: str) -> str:
    """Sanitize + truncate ``value`` for log-line embedding.

    Uses the wire-layer ``sanitize_for_log`` (escapes LF/TAB, unlike
    base's variant) since these sites embed server-supplied idents into
    log records where a forged LF/TAB would split the record.
    """
    sanitised = _sanitize_for_log(value)
    if len(sanitised) <= _LOG_TRUNCATE_MAX_CHARS:
        return sanitised
    overflow = len(sanitised) - _LOG_TRUNCATE_MAX_CHARS
    return f"{sanitised[:_LOG_TRUNCATE_MAX_CHARS]}... [truncated, {overflow} chars]"


# Per-session DB-name suffix so each pytest run gets a fresh namespace
# (dqlite has no DROP DATABASE). Per-worker uniqueness comes from
# _format_url's ``ident`` suffix; under fork-based xdist workers inherit
# the controller's pid verbatim, so the pid is only defensive padding.
_SESSION_TOKEN: Final[str] = f"sa_{os.getpid()}_{time.monotonic_ns()}"

logger = logging.getLogger(__name__)


# Driver names SA's generate_driver_url hook is invoked with (the
# dialect's ``driver`` class attribute, never the entry-point name).
# The bare "dqlite" alias is deliberately excluded so a doubled-dialect
# typo like ``dqlite+dqlite://`` is rejected by the get_dialect() guard.
_DRIVERNAMES: Final[frozenset[str]] = frozenset({"dqlitedbapi", "aio"})


# Scrub @, path separators, Cc control chars, and U+2028/U+2029 from a
# follower ident before it joins the DB name — these are cross-layer
# hazards (URL userinfo/path parsing, wire encode_text, CWE-117 log
# splitting). Not an allowlist, so idents like ``gw0.1`` stay readable.
_IDENT_SCRUB_RE: Final[re.Pattern[str]] = re.compile(r"[@/\\\x00-\x1f\x7f-\x9f  ]")


def _sanitise_ident(ident: str) -> str:
    """Replace each @ / path-separator / Cc control / line-separator code point with ``_``."""
    return _IDENT_SCRUB_RE.sub("_", ident)


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
        existing = url.drivername.split("+", 1)
        driver = existing[1] if len(existing) == 2 else "dqlite"

    # dqlite/dqlitedbapi map to the bare sync form; aio stays explicit.
    new_drivername = "dqlite" if driver in ("dqlite", "dqlitedbapi") else f"dqlite+{driver}"

    database = url.database or "default"
    # SA calls _format_url twice on the same chain (generate_driver_url
    # with ident=None, then follower_url_from_main with the ident); the
    # substring guard stops the session token being appended twice.
    # Substring (not ==/endswith) is load-bearing: a cross-process reaper
    # has a NEW token, must not match the original session's DB, and
    # correctly re-suffixes into its own scratch namespace.
    if _SESSION_TOKEN in database:
        if ident:
            ident_clean = _sanitise_ident(ident)
            ident_suffix = f"_{ident_clean}"
            if not database.endswith(ident_suffix):
                database = f"{database}{ident_suffix}"
    else:
        suffix = _SESSION_TOKEN
        if ident:
            ident_clean = _sanitise_ident(ident)
            suffix = f"{suffix}_{ident_clean}"
        database = f"{database}_{suffix}"

    # Compliance fixtures run DDL on one connection while another holds
    # an open tx; under the dbapi's default immediate (writer-safe) mode
    # the second connection BUSY-blocks until timeout. The suite assumes
    # SQLite's DEFERRED default, so opt these test engines out via the
    # URL query; end-user URLs bypass this helper and keep writer-safe.
    rewritten_query = dict(url.query)
    rewritten_query.setdefault("session_mode", "deferred")
    return url.set(drivername=new_drivername, database=database, query=rewritten_query)


@generate_driver_url.for_db("dqlite")
def _dqlite_generate_driver_url(
    url: sa_url.URL, driver: str, query_str: str | None
) -> sa_url.URL | None:
    """Rewrite ``url`` for the requested driver, or ``None`` if unrecognised."""
    if driver not in _DRIVERNAMES:
        return None
    rewritten = _format_url(url, driver, None)
    # Fail fast on unrecognised dialect drivers; SA's helper would
    # otherwise defer NoSuchModuleError to engine construction.
    try:
        rewritten.get_dialect()
    except Exception:
        return None
    return rewritten


@follower_url_from_main.for_db("dqlite")
def _dqlite_follower_url_from_main(url: sa_url.URL, ident: str) -> sa_url.URL:
    """Rewrite the main URL for a parallel-test follower (each gets its own DB name)."""
    return _format_url(url, None, ident)


@create_db.for_db("dqlite")
def _dqlite_create_db(cfg: Any, eng: Any, ident: str) -> None:
    """No-op: dqlite materialises a database on first ``Open`` request."""
    logger.info("dqlite create_db: no-op for ident=%s", _safe_for_log(str(ident)))


# Drop order by dependency: triggers, views, indexes, then tables last
# (so child-before-parent FK ordering need not be computed). ``{q}`` is
# the already-quoted ident.
_DROP_ORDER: Final[tuple[tuple[str, str], ...]] = (
    ("trigger", "DROP TRIGGER IF EXISTS {q}"),
    ("view", "DROP VIEW IF EXISTS {q}"),
    ("index", "DROP INDEX IF EXISTS {q}"),
    ("table", "DROP TABLE IF EXISTS {q}"),
)


def _drop_user_tables(eng: Any) -> None:
    """Best-effort drop of every user-visible schema object (triggers, views, indexes, tables).

    dqlite has no ``DROP DATABASE``; this is the closest equivalent.
    FK enforcement is disabled for the window so parent/child drop order
    need not be computed, and each DROP is committed individually so one
    failure does not roll back earlier successes.
    """
    try:
        with eng.connect() as conn:
            # Restored in finally even if the DROP loop raises mid-way.
            conn.exec_driver_sql("PRAGMA foreign_keys = OFF")
            try:
                for obj_type, drop_tmpl in _DROP_ORDER:
                    rows = conn.exec_driver_sql(
                        "SELECT name FROM sqlite_master WHERE type=? AND name NOT LIKE 'sqlite_%'",
                        (obj_type,),
                    ).fetchall()
                    for (name,) in rows:
                        # Escape embedded ``"`` so a name like ``foo"bar``
                        # cannot break the DROP (and get masked as a swallowed error).
                        quoted = '"' + name.replace('"', '""') + '"'
                        try:
                            conn.exec_driver_sql(drop_tmpl.format(q=quoted))
                            conn.commit()
                        except Exception as e:
                            # CWE-117: exception text and the sqlite_master
                            # name can carry peer-supplied LF/CR.
                            logger.debug(
                                "drop_user_objects: %s on DROP %s %s",
                                _safe_for_log(str(e)),
                                obj_type,
                                _safe_for_log(str(name)),
                            )
                            with contextlib.suppress(Exception):
                                conn.rollback()
                # WARNING (not DEBUG) so an incomplete cleanup is observable.
                remaining = conn.exec_driver_sql(
                    "SELECT count(*) FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' "
                    "AND type IN ('table','view','trigger','index')"
                ).scalar()
                if remaining:
                    logger.warning(
                        "drop_user_objects: %d user-visible object(s) survived cleanup",
                        remaining,
                    )
            finally:
                conn.exec_driver_sql("PRAGMA foreign_keys = ON")
    except Exception as e:
        logger.debug(
            "drop_user_objects: %s during connect/exec",
            _safe_for_log(str(e)),
        )


@drop_db.for_db("dqlite")
def _dqlite_drop_db(cfg: Any, eng: Any, ident: str) -> None:
    """Drop user tables so the per-follower database is logically empty (no real DROP DATABASE)."""
    logger.info("dqlite drop_db: dropping user tables for ident=%s", _safe_for_log(str(ident)))
    _drop_user_tables(eng)


@drop_all_schema_objects_post_tables.for_db("dqlite")
def _dqlite_drop_all_schema_objects_post_tables(cfg: Any, eng: Any) -> None:
    """Drop any user-visible object the suite created but didn't track. Idempotent."""
    _drop_user_tables(eng)


@run_reap_dbs.for_db("dqlite")
def _dqlite_run_reap_dbs(url: str | sa_url.URL, idents: list[str]) -> None:
    """Reap follower databases at suite end by dropping their user tables."""
    from sqlalchemy import create_engine

    parsed = sa_url.make_url(url) if isinstance(url, str) else url
    # hide_password=True (CWE-532: __str__ default leaks it) + sanitize
    # the host portion (CWE-117 log splitting).
    safe_url = _sanitize_for_log(parsed.render_as_string(hide_password=True))
    logger.info("dqlite reap_dbs: %d follower(s) at %s", len(idents), safe_url)
    for ident in idents:
        # Force the sync driver: _drop_user_tables uses sync connect(),
        # so a dqlite+aio:// input must not route to the async dialect.
        follower_url = _format_url(parsed, "dqlitedbapi", ident)
        try:
            eng = create_engine(follower_url)
            try:
                _drop_user_tables(eng)
            finally:
                eng.dispose()
        except Exception as e:
            # CWE-117: peer-supplied text can reach str(e).
            logger.debug(
                "reap_dbs ident=%s: %s",
                _safe_for_log(str(ident)),
                _safe_for_log(str(e)),
            )


@stop_test_class_outside_fixtures.for_db("dqlite")
def _dqlite_stop_test_class_outside_fixtures(config: Any, db: Any, cls: Any) -> None:
    """Dispose the engine after each test class to release cluster sockets promptly."""
    db.dispose()


@temp_table_keyword_args.for_db("dqlite")
def _dqlite_temp_table_keyword_args(cfg: Any, eng: Any) -> dict[str, Any]:
    """Use SQLite ``TEMPORARY`` prefix; per-connection temp scoping is load-bearing for xdist."""
    return {"prefixes": ["TEMPORARY"]}


@post_configure_engine.for_db("dqlite")
def _dqlite_post_configure_engine(url: Any, engine: Any, follower_ident: Any) -> None:
    """Attach a connect listener enabling FK enforcement for the compliance suite only.

    The production dialect deliberately leaves FK enforcement to the
    user; without this listener the suite's FK batteries would run
    against an FK-disabled connection and never fire IntegrityError.
    """
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
        finally:
            cursor.close()


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
    """Build an upsert for SA's test harness, reusing pysqlite's ``insert`` (dqlite IS SQLite)."""
    from sqlalchemy.dialects.sqlite import insert

    stmt = insert(table)
    if set_lambda is not None:
        stmt = stmt.on_conflict_do_update(set_=set_lambda(stmt.excluded))
    else:
        stmt = stmt.on_conflict_do_nothing()
    stmt = stmt.returning(*returning, sort_by_parameter_order=sort_by_parameter_order)
    return stmt
