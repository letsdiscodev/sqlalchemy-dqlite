"""Coverage for Time columns (ISSUE-20) and metadata reflection (ISSUE-22).

- ISSUE-20: ``Column(Time)`` is not overridden in the dialect's
  ``colspecs`` because the C server tags only DATETIME/DATE/TIMESTAMP
  as ``DQLITE_ISO8601``; TIME columns come back as plain TEXT and the
  inherited SQLAlchemy ``Time`` processor parses the string. Verified
  here end-to-end.

- ISSUE-22: the dialect inherits SQLite's reflection methods, which
  lean on PRAGMA queries. dqlite wraps SQLite but we had zero proof
  that reflection round-trips over the wire. These tests exercise the
  common paths.
"""

import datetime
from collections.abc import Generator

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Time,
    create_engine,
    inspect,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()


class TimeTestModel(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "time_test"
    id = Column(Integer, primary_key=True)
    t = Column(Time)


@pytest.mark.integration
class TestTimeColumn:
    @pytest.fixture
    def engine(self, engine_url: str) -> Generator[Engine]:
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_time_roundtrip(self, engine: Engine) -> None:
        value = datetime.time(10, 30, 45)
        with Session(engine) as s:
            s.add(TimeTestModel(t=value))
            s.commit()
            result = s.query(TimeTestModel).order_by(TimeTestModel.id.desc()).first()
            assert result is not None
            assert isinstance(result.t, datetime.time)
            assert result.t == value


@pytest.mark.integration
class TestReflection:
    """Metadata reflection via PRAGMA round-trips through the wire."""

    def test_reflect_round_trip(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        try:
            # Create a schema with a handful of features.
            src = MetaData()
            Table(
                "authors",
                src,
                Column("id", Integer, primary_key=True),
                Column("name", String(64), nullable=False),
            )
            Table(
                "books",
                src,
                Column("id", Integer, primary_key=True),
                Column("title", String(255), nullable=False),
                Column("author_id", Integer, ForeignKey("authors.id")),
                Column("published", Date),
                Column("updated_at", DateTime),
                Index("ix_books_title", "title"),
            )
            src.create_all(engine)
            try:
                # Reflect into a fresh MetaData and check the shape.
                dst = MetaData()
                dst.reflect(bind=engine)
                assert {"authors", "books"}.issubset(dst.tables.keys())

                authors = dst.tables["authors"]
                assert "id" in authors.columns
                assert "name" in authors.columns
                assert list(authors.primary_key.columns.keys()) == ["id"]

                books = dst.tables["books"]
                assert {"id", "title", "author_id", "published", "updated_at"}.issubset(
                    books.columns.keys()
                )

                # Inspector-level checks.
                insp = inspect(engine)
                cols = {c["name"] for c in insp.get_columns("books")}
                assert cols == {"id", "title", "author_id", "published", "updated_at"}

                indexes = insp.get_indexes("books")
                index_names = {ix["name"] for ix in indexes}
                assert "ix_books_title" in index_names

                fks = insp.get_foreign_keys("books")
                assert any(
                    fk["referred_table"] == "authors"
                    and fk["constrained_columns"] == ["author_id"]
                    for fk in fks
                )
            finally:
                src.drop_all(engine)
        finally:
            engine.dispose()
