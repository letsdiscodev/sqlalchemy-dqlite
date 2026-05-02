"""SA testing-provision ``upsert`` hook is registered for ``dqlite``.

The hook is consumed by SA's internal full test suite (and by
third-party tooling that introspects the registry); the public
compliance suite under ``sqlalchemy.testing.suite`` does not currently
exercise it. Registering the hook is parity with the other shipped
backends (sqlite, postgresql, mysql) and future-proofs against SA
promoting the upsert tests into the public suite.
"""

import sqlalchemydqlite.provision  # noqa: F401  side-effect registration


def test_upsert_provision_hook_registered_for_dqlite() -> None:
    from sqlalchemy.testing.provision import upsert

    assert "dqlite" in upsert.fns, (
        "upsert hook for ``dqlite`` not registered; SA OnConflictTest / "
        "third-party introspection by backend name will silently miss "
        "our dialect"
    )


def test_upsert_provision_hook_returns_sqlite_insert_variant() -> None:
    """Body parity with the pysqlite reference: re-uses
    ``sqlalchemy.dialects.sqlite.insert`` so the inherited
    ``SQLiteCompiler.visit_on_conflict_do_*`` path runs at compile.
    """
    from sqlalchemy import Column, Integer, MetaData, String, Table
    from sqlalchemy.dialects.sqlite import Insert as SQLiteInsert
    from sqlalchemy.testing.provision import upsert

    md = MetaData()
    t = Table(
        "t",
        md,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )
    fn = upsert.fns["dqlite"]
    stmt = fn(None, t, [t.c.id], set_lambda=lambda excluded: {"name": excluded.name})
    assert isinstance(stmt, SQLiteInsert)
