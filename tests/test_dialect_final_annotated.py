"""Pin: ``sqlalchemydqlite.dialect`` and ``.dialect_aio`` are ``Final``-annotated aliases."""


def test_dialect_alias_is_final_annotated() -> None:
    import sqlalchemydqlite as M

    assert "dialect" in M.__annotations__, (
        "sqlalchemydqlite.dialect must be Final-annotated — workspace "
        "discipline mirroring __version__-Final"
    )
    ann = M.__annotations__["dialect"]
    assert "Final[" in str(ann), f"dialect annotation is not Final[...] — got {ann!r}"


def test_dialect_aio_alias_is_final_annotated() -> None:
    import sqlalchemydqlite as M

    assert "dialect_aio" in M.__annotations__, (
        "sqlalchemydqlite.dialect_aio must be Final-annotated — workspace "
        "discipline mirroring __version__-Final"
    )
    ann = M.__annotations__["dialect_aio"]
    assert "Final[" in str(ann), f"dialect_aio annotation is not Final[...] — got {ann!r}"


def test_dialect_aliases_point_at_correct_classes() -> None:
    import sqlalchemydqlite as M
    from sqlalchemydqlite.aio import DqliteDialect_aio
    from sqlalchemydqlite.base import DqliteDialect

    assert M.dialect is DqliteDialect
    assert M.dialect_aio is DqliteDialect_aio
