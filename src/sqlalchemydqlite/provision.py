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

import contextlib
import logging
import os
import re
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

# Match the discipline used by ``_safe_for_log`` in ``base.py``:
# sanitize control / bidi / invisible characters AND truncate so a
# server-controlled value (e.g. a table name read from ``sqlite_master``
# in a multi-tenant cluster, or an error message bubbling up from the
# wire) cannot allocate an unbounded log record.
_LOG_TRUNCATE_MAX_CHARS: Final[int] = 200


def _safe_for_log(value: str) -> str:
    """Sanitize + truncate ``value`` for log-line embedding.

    Composes the wire-layer ``sanitize_for_log`` helper (strips C0 /
    C1, U+2028 / U+2029, full bidi block, ZW chars, BOM AND escapes
    LF / TAB so journald / syslog cannot interpret either as a
    record boundary) with a local 200-char truncate cap so the
    resulting log record is bounded regardless of server-supplied
    input size.

    **Stronger than ``base._safe_for_log``**, which uses
    ``_sanitize_server_text`` (strips controls but does NOT escape
    LF / TAB — appropriate for exception-message embedding where
    LF aids readability). Provision call sites embed
    server-supplied identifier strings (table names from
    ``sqlite_master``, etc.) into ``logger.debug`` records where a
    forged LF / TAB would split the record; the stronger escape is
    load-bearing here.

    Defined locally to avoid the provision → base import direction.
    """
    sanitised = _sanitize_for_log(value)
    if len(sanitised) <= _LOG_TRUNCATE_MAX_CHARS:
        return sanitised
    overflow = len(sanitised) - _LOG_TRUNCATE_MAX_CHARS
    return f"{sanitised[:_LOG_TRUNCATE_MAX_CHARS]}... [truncated, {overflow} chars]"


# Per-session unique database-name suffix. dqlite has no
# ``DROP DATABASE``: reusing the cluster's ``default`` database across
# pytest sessions would leak prior-session schema (every table the
# suite or our other integration tests created). Append a session-
# unique token at URL-rewrite time so each pytest run gets a fresh,
# empty database namespace on the cluster.
#
# Token shape: ``sa_<importing-process-pid>_<monotonic-ns>`` — bounded
# length, safe across the dbapi/URL/wire layers (no path separators,
# no ``@``, no whitespace), and sufficiently unique for parallel
# pytest runs that share a cluster fixture. Module-level so the value
# is stable across every URL rewrite within a single pytest session.
#
# **Fork note.** Under fork-based ``pytest-xdist`` (or any
# ``multiprocessing.fork`` context), worker processes inherit the
# token verbatim — the ``<pid>`` component embeds the IMPORTING
# process's pid, which is the xdist controller's, not the worker's.
# Per-worker uniqueness is provided by :func:`_format_url`'s
# ``ident`` suffix (e.g. ``gw0`` / ``gw1``); the pid is defensive
# overprovisioning, not the primary isolation token. No
# ``os.register_at_fork`` refresh hook is registered because the
# existing ``_SESSION_TOKEN in database`` idempotence check below
# relies on cross-process URL passthrough remaining stable.
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
# Accept the two SA-invoked driver-name forms so ``--dburi`` /
# ``--dbs`` work with any canonical URL shape:
#
#   dqlite://...               -> driver name "dqlitedbapi"
#   dqlite+dqlitedbapi://...   -> driver name "dqlitedbapi"
#   dqlite+aio://...           -> driver name "aio"
#
# We do NOT accept the bare "dqlite" alias in this allowlist. A user-
# supplied URL ``dqlite+dqlite://host/db`` (a typo where the dialect
# name is doubled) would otherwise be silently accepted as
# equivalent to ``dqlite://host/db`` — the explicit
# ``rewritten.get_dialect()`` fail-fast at the bottom of
# ``_dqlite_generate_driver_url`` exists precisely to catch
# misconfiguration, and tolerating the duplicate alias here defeats
# that guard. SA never invokes the hook with the bare driver name
# at the call site (``URL.get_driver_name()`` always returns the
# dialect's ``driver`` class attribute, never the entry-point name).
_DRIVERNAMES: Final[frozenset[str]] = frozenset({"dqlitedbapi", "aio"})


# Characters scrubbed from the follower ident before it is appended to
# the database name. The earlier strip only handled ``/`` and ``@`` —
# the surrounding narrative comment at the call site promised ``@`` /
# path-separators / control chars, which the two-character strip did
# not deliver. Widen the scrub to match the documented intent:
#
# * ``@`` and ``/`` — the original two; userinfo and unix path-
#   separator interpretation by intermediate URL parsers and the
#   server's database-name field.
# * ``\\`` — windows path-separator; the comment's "path-separators"
#   plural was load-bearing.
# * Control chars across the full Unicode ``Cc`` category: C0
#   (``\\x00``..``\\x1f``), DEL (``\\x7f``), and C1
#   (``\\x80``..``\\x9f``). Split log records that echo the URL
#   (CWE-117 log-record-splitting), late-reject at the wire's
#   ``encode_text`` for NUL with an obscure error far from the
#   ident source, route TAB as a TSV-encoded column separator in
#   structured loggers, and cover NEL (``\\x85``) which
#   ``str.splitlines`` treats as a line terminator alongside
#   ``\\n`` / ``\\r``. DEL is the seventh-bit-only C0 trailer that
#   shares the TTY / terminal-control hazards of the C0 range.
# * U+2028 / U+2029 — LINE SEPARATOR / PARAGRAPH SEPARATOR; treated
#   as LF-equivalent by journald and many JSON log encoders.
#
# Other characters (alphanumerics, dots, dashes, underscores, ...)
# survive verbatim. This is NOT a strict ``[A-Za-z0-9_]`` allowlist:
# legitimate idents with version suffixes (``gw0.1``) or hyphens
# stay readable, the scrub is scoped to characters the comment
# enumerates as cross-layer hazards.
_IDENT_SCRUB_RE: Final[re.Pattern[str]] = re.compile(r"[@/\\\x00-\x1f\x7f-\x9f  ]")


def _sanitise_ident(ident: str) -> str:
    """Replace each ``@`` / path-separator / control-char / line-
    separator code point in ``ident`` with ``_``.

    Scoped to test-fixture idents (pytest-xdist worker tokens such as
    ``gw0`` / ``gw1`` and any custom follower idents wired into the
    SA provision plugin). Shared by both arms of ``_format_url`` so
    the already-suffixed branch and the fresh-suffix branch carry the
    same policy. The scrub is conservative (replace-only, no
    allowlist) so legitimate custom idents with dots / hyphens
    survive.

    "Control-char" coverage spans the full Unicode ``Cc`` category:
    C0 (``\\x00``..``\\x1f``), DEL (``\\x7f``), and C1
    (``\\x80``..``\\x9f``). NEL (``\\x85``) and DEL are explicitly
    included so the docstring's plural "control chars" promise
    matches the implementation; per ``str.splitlines`` NEL is also a
    line terminator and shares the log-record-splitting hazard with
    LF / CR / U+2028 / U+2029.
    """
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
    #
    # The ``in`` substring check (rather than ``==`` /
    # ``.endswith()``) is load-bearing across two distinct caller
    # populations:
    #
    # 1. Same-process re-entry — the controller / fork-inherited
    #    worker calls ``_format_url`` twice with the SAME
    #    ``_SESSION_TOKEN``; ``substring`` and ``equality`` both
    #    match.
    # 2. Cross-process ``--reap-dbs`` — the reaper CLI is a freshly
    #    spawned process; its ``_SESSION_TOKEN`` is NEW (different
    #    pid + monotonic_ns). The reaper's ``_SESSION_TOKEN in
    #    database`` check against the ORIGINAL session's
    #    token-in-database returns False → bottom branch → re-suffix
    #    with the reaper's token. This is the correct behaviour:
    #    the reaper operates on its own scratch namespace and
    #    leaves the original session's databases untouched.
    #
    # A future "correctness" refactor tightening this to ``==`` /
    # ``.endswith()`` / regex-extract-and-compare-exact would
    # silently break case (2) — the reaper would never match an
    # original-session DB and the reap cascade would append a fresh
    # token per invocation, producing un-cleanable
    # ``db_<reaper1>_<reaper2>_...`` chains. Keep the substring
    # check; the fork-note block at the top of this module documents
    # the fork surface, this comment documents the cross-process
    # reap surface.
    if _SESSION_TOKEN in database:
        # Prior pass already attached the session token. Append the
        # follower ident only.
        if ident:
            ident_clean = _sanitise_ident(ident)
            ident_suffix = f"_{ident_clean}"
            if not database.endswith(ident_suffix):
                database = f"{database}{ident_suffix}"
    else:
        suffix = _SESSION_TOKEN
        if ident:
            # Scrub ``@`` / path-separators / control chars / line-
            # separators so the resulting name parses cleanly across
            # the dbapi / URL / wire layers (CWE-117 log-record-
            # splitting via control chars, defense-in-depth against
            # unusual fixture configs). See ``_sanitise_ident`` for
            # the full character list and rationale.
            ident_clean = _sanitise_ident(ident)
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
    # Sanitise ``ident`` through ``_safe_for_log`` (strict variant —
    # escapes LF/TAB and strips U+2028/U+2029/bidi controls) so a
    # forged ident produced by a third-party follower_ident_fn cannot
    # splice the operator log. Sibling ``reap_dbs`` site at the
    # bottom of this module already routes through the same helper;
    # this brings the three provision log sites into uniform
    # discipline.
    logger.info("dqlite create_db: no-op for ident=%s", _safe_for_log(str(ident)))


# Object-type drop order: triggers reference tables/views; views
# reference tables; indexes are auto-dropped with their tables, but a
# standalone or shadow-table-attached index is explicitly handled to
# match the SQLite documentation contract. Tables drop last so the
# child-before-parent FK ordering need not be explicitly computed.
# Format string templates use ``{q}`` for the already-quoted ident.
_DROP_ORDER: Final[tuple[tuple[str, str], ...]] = (
    ("trigger", "DROP TRIGGER IF EXISTS {q}"),
    ("view", "DROP VIEW IF EXISTS {q}"),
    ("index", "DROP INDEX IF EXISTS {q}"),
    ("table", "DROP TABLE IF EXISTS {q}"),
)


def _drop_user_tables(eng: Any) -> None:
    """Best-effort drop of every user-visible schema object in the
    dialect's default schema — triggers, views, indexes, and tables.

    dqlite has no ``DROP DATABASE``; this is the closest equivalent.
    Used by ``drop_db``, ``drop_all_schema_objects_post_tables``, and
    ``run_reap_dbs``. The cluster fixture is the authoritative
    reset boundary; this helper makes the per-database state
    logically empty between runs.

    Three behavioural disciplines the prior implementation lacked:

    * Foreign-key enforcement is turned off for the cleanup window
      (``PRAGMA foreign_keys = OFF``) and restored in a ``finally``.
      Without this, ``DROP TABLE parent`` while ``child`` still
      holds an FK reference raises and the per-drop swallow turns
      the FK violation into a DEBUG log — the parent leaks.
    * Each successful DROP is committed individually. The previous
      single ``conn.commit()`` at the bottom rolled back every prior
      successful DROP whenever a single failure poisoned the
      autobegin transaction (and the outer ``except`` then
      DEBUG-logged the commit failure, hiding the cascade).
    * The ``sqlite_master.type`` filter covers triggers / views /
      indexes / tables in dependency order — the previous helper
      filtered ``type='table'`` only and the module docstring's
      "every user-visible schema object" promise was broken for
      the other three types. The ``drop_all_schema_objects_post_tables``
      SA hook that delegates here is now functional rather than a
      no-op.

    A survivor probe runs after the loop: if any user-visible object
    survives, log at WARNING (NOT DEBUG) so operators see the
    cleanup-incomplete signal.
    """
    try:
        with eng.connect() as conn:
            # Cleanup-window FK pragma flip; restored in finally even
            # if a DROP loop raises mid-way.
            conn.exec_driver_sql("PRAGMA foreign_keys = OFF")
            try:
                for obj_type, drop_tmpl in _DROP_ORDER:
                    rows = conn.exec_driver_sql(
                        "SELECT name FROM sqlite_master WHERE type=? AND name NOT LIKE 'sqlite_%'",
                        (obj_type,),
                    ).fetchall()
                    for (name,) in rows:
                        # SQLite escapes ``"`` inside a delimited
                        # identifier as ``""``. A peer / test creating
                        # an object named ``foo"bar`` would otherwise
                        # render as a syntax error that the per-drop
                        # swallow would mask, silently breaking the
                        # docstring's "drop every user-visible
                        # schema object" contract.
                        quoted = '"' + name.replace('"', '""') + '"'
                        try:
                            conn.exec_driver_sql(drop_tmpl.format(q=quoted))
                            # Commit per-drop: a later failure must
                            # not roll back earlier successes via the
                            # autobegin transaction. Pairs with the
                            # rollback in the per-drop except below.
                            conn.commit()
                        except Exception as e:
                            # CWE-117: both the bubbled-up exception
                            # text and the object ``name`` from
                            # ``sqlite_master`` can carry peer-supplied
                            # LF/CR. Route both through
                            # ``sanitize_for_log`` so journald/syslog
                            # cannot interpret forged LF as a record
                            # boundary.
                            logger.debug(
                                "drop_user_objects: %s on DROP %s %s",
                                _safe_for_log(str(e)),
                                obj_type,
                                _safe_for_log(str(name)),
                            )
                            # Rollback failure on the cleanup path is
                            # itself non-fatal — the next-drop's
                            # autobegin fires afresh against a fresh
                            # transaction state once the connection
                            # sees the next exec_driver_sql.
                            with contextlib.suppress(Exception):
                                conn.rollback()
                # Survivor probe: a non-empty residual signals the
                # cleanup left work undone. Operator-visible WARNING
                # (not DEBUG) so the gap is observable.
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
    """Drop user tables from the per-follower database.

    dqlite cannot drop the database itself; the database name persists
    on the cluster until the cluster is recycled. Drop all user tables
    so the database is logically empty — the next test run that picks
    the same name sees a clean schema.
    """
    # See ``create_db`` for the ``_safe_for_log`` rationale.
    logger.info("dqlite drop_db: dropping user tables for ident=%s", _safe_for_log(str(ident)))
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
    # CWE-532 (credential leakage) + CWE-117 (log injection on the host
    # portion). ``URL.__str__`` defaults to
    # ``render_as_string(hide_password=False)`` — the password lands in
    # the INFO record verbatim. Use the explicit ``hide_password=True``
    # form to render the password as ``***``. The host portion is then
    # routed through ``sanitize_for_log`` so a peer- or
    # operator-supplied URL whose host carries LF / U+2028 / bidi /
    # ZWSP cannot split the log record. Mirrors the discipline at the
    # sibling DEBUG sites in this file (``_drop_user_tables`` and the
    # reap-loop per-ident error path).
    safe_url = _sanitize_for_log(parsed.render_as_string(hide_password=True))
    logger.info("dqlite reap_dbs: %d follower(s) at %s", len(idents), safe_url)
    for ident in idents:
        # Force the sync drivername regardless of the input URL's
        # ``+driver`` suffix. ``_drop_user_tables`` uses sync
        # ``eng.connect()`` — if the input was ``dqlite+aio://``,
        # the unforced ``_format_url(parsed, None, ident)`` would
        # carry the ``+aio`` suffix and ``create_engine`` would
        # route to the async dialect that requires
        # ``create_async_engine``. The sync rewrite via the
        # ``"dqlitedbapi"`` driver token resolves to the bare
        # ``"dqlite"`` drivername (``_format_url``'s
        # ``driver in ("dqlite", "dqlitedbapi")`` branch maps
        # both to the bare form).
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
                _safe_for_log(str(ident)),
                _safe_for_log(str(e)),
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
