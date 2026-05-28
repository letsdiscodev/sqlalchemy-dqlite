"""Integration tests for supports_native_boolean = True.

DDL for ``Column(Boolean)`` must not emit ``CHECK (col IN (0, 1))``
because dqlite's wire protocol enforces the invariant natively.
"""

from collections.abc import Generator

import pytest
from sqlalchemy import Boolean, Column, Integer, create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()


class BoolModel(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "native_bool_test"
    id = Column(Integer, primary_key=True)
    flag = Column(Boolean, nullable=False)


class NullableBoolModel(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "native_bool_nullable_test"
    id = Column(Integer, primary_key=True)
    flag = Column(Boolean, nullable=True)


@pytest.mark.integration
class TestNativeBoolean:
    @pytest.fixture
    def engine(self, engine_url: str) -> Generator[Engine]:
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_ddl_does_not_emit_check_constraint(self, engine: Engine) -> None:
        """With supports_native_boolean=True, no CHECK clause should appear."""
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT sql FROM sqlite_master WHERE type='table' AND name='native_bool_test'")
            ).first()
            assert result is not None
            ddl = result[0].upper()
            assert "CHECK" not in ddl, f"Unexpected CHECK in DDL: {result[0]}"

    def test_roundtrip_preserves_native_bool(self, engine: Engine) -> None:
        with Session(engine) as s:
            s.add_all([BoolModel(flag=True), BoolModel(flag=False)])
            s.commit()
            rows = s.query(BoolModel).order_by(BoolModel.id).all()
            assert [r.flag for r in rows] == [True, False]
            assert all(isinstance(r.flag, bool) for r in rows)

    def test_nullable_boolean_preserves_none(self, engine: Engine) -> None:
        """A NULL in a nullable Boolean column reads back as ``None``,
        not ``False`` — the NULL-vs-FALSE distinction must survive the
        ORM layer, not just the raw client/DBAPI layers.
        """
        with Session(engine) as s:
            s.add_all(
                [
                    NullableBoolModel(id=1, flag=True),
                    NullableBoolModel(id=2, flag=False),
                    NullableBoolModel(id=3, flag=None),
                ]
            )
            s.commit()
            rows = s.query(NullableBoolModel).order_by(NullableBoolModel.id).all()
            assert rows[0].flag is True
            assert rows[1].flag is False
            assert rows[2].flag is None

    def test_reflect_column_is_boolean(self, engine: Engine) -> None:
        insp = inspect(engine)
        cols = {c["name"]: c for c in insp.get_columns("native_bool_test")}
        assert "flag" in cols
        # SQLAlchemy should be able to reflect the BOOLEAN type back.
        # We accept either Boolean or the inherited SQLite numeric
        # fallback, but assert type_affinity is Boolean.
        from sqlalchemy import types

        assert cols["flag"]["type"].python_type is bool or isinstance(
            cols["flag"]["type"], types.Boolean
        )
        # Non-null reflection works.
        assert cols["flag"]["nullable"] is False


@pytest.mark.integration
class TestDialectFlag:
    def test_supports_native_boolean_is_true(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        try:
            assert engine.dialect.supports_native_boolean is True
        finally:
            engine.dispose()
