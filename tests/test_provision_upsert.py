"""SA testing-provision ``upsert`` hook is registered for ``dqlite``."""

import sqlalchemydqlite.provision  # noqa: F401  side-effect registration


def test_upsert_provision_hook_registered_for_dqlite() -> None:
    from sqlalchemy.testing.provision import upsert

    assert "dqlite" in upsert.fns, (
        "upsert hook for ``dqlite`` not registered; SA OnConflictTest / "
        "third-party introspection by backend name will silently miss "
        "our dialect"
    )


def test_upsert_provision_hook_returns_sqlite_insert_variant() -> None:
    """Re-uses ``sqlalchemy.dialects.sqlite.insert`` so the inherited
    ``SQLiteCompiler.visit_on_conflict_do_*`` path runs at compile."""
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
