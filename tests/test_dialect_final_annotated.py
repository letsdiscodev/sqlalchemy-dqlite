"""Pin: ``sqlalchemydqlite.dialect`` and ``.dialect_aio`` resolve to the correct classes."""


def test_dialect_aliases_point_at_correct_classes() -> None:
    import sqlalchemydqlite as M
    from sqlalchemydqlite.aio import DqliteDialect_aio
    from sqlalchemydqlite.base import DqliteDialect

    assert M.dialect is DqliteDialect
    assert M.dialect_aio is DqliteDialect_aio
